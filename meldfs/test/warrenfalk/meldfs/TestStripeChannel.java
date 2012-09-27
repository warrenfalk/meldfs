package warrenfalk.meldfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

public class TestStripeChannel {

	@Test
	public void testReadByteBufferFull() throws IOException {
		long testSize = 26;
		ExecutorService pool = Executors.newCachedThreadPool();
		TestColumn[] testColumns = createTestColumns(testSize, 3, 5, 5);
		StripeChannel subject = new StripeChannel(pool, testSize, 3, 5, testColumns, -1);
		byte[] bytes = new byte[27];
		ByteBuffer dst = ByteBuffer.wrap(bytes);
		int returnVal = subject.read(dst);
		assertEquals("return value of read()", testSize, returnVal);
		assertEquals("buffer position after read()", testSize, dst.position());
		assertEquals("buffer limit after read()", bytes.length, dst.limit());
		assertEquals("column[0] position after read()", testColumns[0].offset, 10);
		assertEquals("column[1] position after read()", testColumns[1].offset, 10);
		assertEquals("column[2] position after read()", testColumns[2].offset, 6);
		returnVal = subject.read(dst);
		assertEquals("return value of read() after EOF", -1, returnVal);
	}
	
	@Test
	public void testReadByteBufferIterative() throws IOException {
		long testSize = 26;
		ExecutorService pool = Executors.newCachedThreadPool();
		TestColumn[] testColumns = createTestColumns(testSize, 3, 5, 5);
		StripeChannel subject = new StripeChannel(pool, testSize, 3, 5, testColumns, -1);
		byte[] bytes = new byte[7];
		ByteBuffer dst = ByteBuffer.wrap(bytes);
		int returnVal;
		returnVal = subject.read(dst);
		assertEquals("return value of read()x1", 7, returnVal);
		assertEquals("buffer position after read()x1", 7, dst.position());
		assertEquals("buffer limit after read()x1", 7, dst.limit());
		assertEquals("column[0] position after read()x1", testColumns[0].offset, 5);
		assertEquals("column[1] position after read()x1", testColumns[1].offset, 2);
		assertEquals("column[2] position after read()x1", testColumns[2].offset, 0);
		dst.clear();
		returnVal = subject.read(dst);
		assertEquals("return value of read()x2", 7, returnVal);
		assertEquals("buffer position after read()x2", 7, dst.position());
		assertEquals("buffer limit after read()x2", 7, dst.limit());
		assertEquals("column[0] position after read()x2", testColumns[0].offset, 5);
		assertEquals("column[1] position after read()x2", testColumns[1].offset, 5);
		assertEquals("column[2] position after read()x2", testColumns[2].offset, 4);
		dst.clear();
		returnVal = subject.read(dst);
		assertEquals("return value of read()x3", 7, returnVal);
		assertEquals("buffer position after read()x3", 7, dst.position());
		assertEquals("buffer limit after read()x3", 7, dst.limit());
		assertEquals("column[0] position after read()x3", testColumns[0].offset, 10);
		assertEquals("column[1] position after read()x3", testColumns[1].offset, 6);
		assertEquals("column[2] position after read()x3", testColumns[2].offset, 5);
		dst.clear();
		returnVal = subject.read(dst);
		assertEquals("return value of read()x3", 5, returnVal);
		assertEquals("buffer position after read()x3", 5, dst.position());
		assertEquals("buffer limit after read()x3", 7, dst.limit());
		assertEquals("column[0] position after read()x3", testColumns[0].offset, 10);
		assertEquals("column[1] position after read()x3", testColumns[1].offset, 10);
		assertEquals("column[2] position after read()x3", testColumns[2].offset, 6);
		dst.clear();
		returnVal = subject.read(dst);
		assertEquals("return value of read() after EOF", -1, returnVal);
	}
	
	TestColumn[] createTestColumns(long totalSize, int dataSize, int blockSize, int count) {
		long fullBlockCount = totalSize / blockSize;
		long blocksMin = fullBlockCount / dataSize;
		long fullBlockRemainder = fullBlockCount % dataSize;
		long partBlockSize = totalSize % blockSize;
		long checksumSize = fullBlockCount * blockSize;
		if (fullBlockRemainder > 0)
			checksumSize += blockSize;
		else
			checksumSize += partBlockSize;
		TestColumn[] c = new TestColumn[count];
		for (int i = 0; i < count; i++) {
			long size = blocksMin * blockSize;
			if (i >= dataSize)
				size = checksumSize;
			else if (i < fullBlockRemainder)
				size += blockSize;
			else if (i == fullBlockRemainder)
				size += partBlockSize;
			c[i] = new TestColumn(i, size);
		}
		return c;
	}
	
	class TestColumn implements ScatteringByteChannel {
		int index;
		long size;
		long offset;
		boolean closed;
		
		TestColumn(int index, long size) {
			this.index = index;
			this.size = size;
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			if (offset >= size)
				return -1;
			int position = dst.position();
			int dataRemain = (int)(size - offset);
			int bufferRemain = dst.remaining();
			int bytes = Math.min(dataRemain, bufferRemain);
			dst.position(position + bytes);
			offset += bytes;
			return bytes;
		}

		@Override
		public boolean isOpen() {
			return !closed;
		}

		@Override
		public void close() throws IOException {
			closed = true;
		}

		@Override
		public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
			long total = 0;
			for (int i = 0; i < length; i++) {
				int bytes = read(dsts[i]);
				if (bytes == -1)
					return total;
				total += bytes;
			}
			return total;
		}

		@Override
		public long read(ByteBuffer[] dsts) throws IOException {
			return read(dsts, 0, dsts.length);
		}
		
	}
}
