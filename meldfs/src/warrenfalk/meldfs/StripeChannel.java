package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides, currently, a single-channel read implementation on multi-channel striped source data.
 * Later we may need to genericize this function so that we can wrap either read, write, or read/write channels.
 * Although that will be significantly more complicated, as writes need some kind of process for checksum calculation (need entire stripes available before checksums can be calculated)
 * @author warren
 *
 */
public class StripeChannel implements ScatteringByteChannel {
	final long size;
	final int dataCount;
	final int blockSize;
	final Column[] columns;
	final long validMask;
	final ExecutorService threadPool;
	final AtomicInteger readOpsIncomplete = new AtomicInteger();
	long position;
	final Semaphore readOpsComplete = new Semaphore(0);

	public StripeChannel(ExecutorService threadPool, long size, int dataCount, int blockSize, ScatteringByteChannel[] columns, long validMask) {
		this.size = size;
		this.columns = new Column[columns.length];
		for (int i = 0; i < columns.length; i++)
			this.columns[i] = new Column(i, 0 != (validMask & (1 << i)), columns[i]);
		this.validMask = validMask;
		this.dataCount = dataCount;
		this.threadPool = threadPool;
		this.blockSize = blockSize;
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if (position == size)
			return -1;
		long all = Math.min(dst.remaining(), size - position);
		long remain = all;
		// divide the buffer among the columns until we run out of buffer or...
		int columnStart = columns.length;
		int columnEnd = -1;
		while (remain > 0) {
			int columnIndex = (int)((position / blockSize) % dataCount);
			if (columnIndex < columnStart)
				columnStart = columnIndex;
			if (columnIndex >= columnEnd)
				columnEnd = columnIndex + 1;
			int blockPos = (int)(position % blockSize);
			int blockRemain = blockSize - blockPos;
			int blockRead = (int)Math.min(remain, blockRemain);
			// ... we run out of column buffer capacity
			if (columns[columnIndex].takeBiteOf(dst, blockRead))
				break;
			dst.position(dst.position() + blockRead);
			remain -= blockRead;
			position += blockRead;
		}
		readOpsIncomplete.set(columnEnd - columnStart);
		if (readOpsIncomplete.intValue() > 0) {
			for (int columnIndex = columnStart; columnIndex < columnEnd; columnIndex++) {
				columns[columnIndex].start();
			}
			readOpsComplete.acquireUninterruptibly();
		}
		return (int)(all - remain);
	}

	@Override
	public boolean isOpen() {
		for (int i = 0; i < columns.length; i++)
			if (!columns[i].online && columns[i].channel.isOpen())
				return false;
		return true;
	}

	@Override
	public void close() throws IOException {
		for (int i = 0; i < columns.length; i++)
			if (columns[i].online)
				columns[i].channel.close();
	}

	@Override
	public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long read(ByteBuffer[] dsts) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private class Column {
		final int index;
		final ScatteringByteChannel channel;
		long remaining;
		ByteBuffer[] buffers;
		int bufferCount;
		boolean online;
		Runnable readOp;
		Exception exception;

		public Column(int index, boolean online, ScatteringByteChannel channel) {
			this.online = online;
			this.index = index;
			this.channel = channel;
			if (online)
				buffers = new ByteBuffer[16];
		}

		public void start() {
			if (readOp == null)
				readOp = new ReadOperation();
			exception = null;
			threadPool.execute(readOp);
		}

		/**
		 * Takes a slice of <code>size</code> size out of the buffer <code>buffer</code> returning true if there is no more buffer list room.
		 */
		public boolean takeBiteOf(ByteBuffer buffer, int size) {
			if (bufferCount == buffers.length)
				return true;
			remaining += size;
			int limit = buffer.limit();
			buffer.limit(buffer.position() + size);
			ByteBuffer slice = buffer.slice();
			buffer.limit(limit);
			buffers[bufferCount++] = slice;
			return false;
		}
		
		private class ReadOperation implements Runnable {
			@Override
			public void run() {
				try {
					while (remaining > 0) {
						long bytes = channel.read(buffers, 0, bufferCount);
						if (bytes == -1)
							throw new IOException("Unexpected EOF in column " + index);
						remaining -= bytes;
					}
				}
				catch (IOException e) {
					exception = e;
				}
				finally {
					for (int i = 0; i < bufferCount; i++)
						buffers[i] = null;
					bufferCount = 0;
					remaining = 0;
					if (readOpsIncomplete.decrementAndGet() == 0)
						readOpsComplete.release();
				}
			}
		}
	}

}
