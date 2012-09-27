package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public class StripeMatrix {
	final ByteBuffer[] stripes;
	final ByteBuffer[][] stripeBlocks;
	final ByteBuffer[][] columns;
	final int dataCount;
	final int blockSize;
	final long totalDataSize;
	
	final static int STRIPE_COUNT = 16; // chosen because this is the maximum number of buffers in a vectored IO operation
	
	public StripeMatrix(int dataCount, int checksumCount, int blockSize) {
		this.dataCount = dataCount;
		this.blockSize = blockSize;
		this.stripes = new ByteBuffer[STRIPE_COUNT];
		this.stripeBlocks = new ByteBuffer[STRIPE_COUNT][];
		this.columns = new ByteBuffer[dataCount + checksumCount][];
		for (int i = 0; i < stripeBlocks.length; i++)
			stripeBlocks[i] = new ByteBuffer[dataCount + checksumCount];
		for (int i = 0; i < columns.length; i++)
			columns[i] = new ByteBuffer[STRIPE_COUNT];
		
		int stripeSize = columns.length * blockSize;
		int bufferSize = STRIPE_COUNT * stripeSize;
		
		// first allocate one big monolithic buffer for everything
		ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
		
		// subdivide the monolithic buffer into stripes
		for (int s = 0; s < STRIPE_COUNT; s++) {
			int start = s * stripeSize;
			int end = start + stripeSize;
			buffer.limit(end);
			buffer.position(start);
			ByteBuffer stripeWhole = buffer.slice();
			// create a data buffer for the stripe
			stripeWhole.limit(dataCount * blockSize);
			stripes[s] = stripeWhole.slice();
			stripeWhole.clear();
			// partition the stripe into blocks, assigning one to each column
			for (int c = 0; c < columns.length; c++) {
				start = c * blockSize;
				end = start + blockSize;
				stripeWhole.limit(end);
				stripeWhole.position(start);
				stripeBlocks[s][c] = columns[c][s] = stripeWhole.slice();
			}
		}
		
		totalDataSize = STRIPE_COUNT * dataCount * blockSize;
	}
	
	public long getTotalDataSize() {
		return totalDataSize;
	}
	
	/**
	 * Reads from an unstriped source channel into the matrix.
	 * <p>Blocks until all stripes are full or the EOF is reached.</p>
	 * <p>(Does not return -1 on EOF, rather if the value returned is less than <code>getTotalDataSize()</code> then eof was reached.)</p>
	 * @param channel
	 * @return the number of bytes read
	 * @throws IOException
	 */
	public long readStripes(ScatteringByteChannel channel) throws IOException {
		long total = 0;
		for (int s = 0; s < stripes.length; s++)
			stripes[s].clear();
		do {
			long bytes = channel.read(stripes);
			if (bytes == -1)
				break;
			total += bytes;
		} while (total < totalDataSize);
		long read = total;
		for (int s = 0; s < stripes.length; s++) {
			int c;
			for (c = 0; c < dataCount; c++)
				read -= columns[c][s].limit((int)Math.min(read, blockSize)).position(0).limit();
			for (; c < columns.length; c++)
				columns[c][s].position(0).limit(0);
		}
		return total;
	}
	
	/**
	 * Reads from one column of a striped source into the matrix.
	 * @param column
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	public long readColumn(int column, ScatteringByteChannel channel) throws IOException {
		ByteBuffer[] blocks = columns[column];
		return channel.read(blocks);
	}
	
	/**
	 * Writes to one column of a stripe from the matrix.
	 * <p>Blocks until all data is written</p>
	 * @param column
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	public long writeColumn(int column, GatheringByteChannel channel) throws IOException {
		ByteBuffer[] blocks = columns[column];
		long all = 0;
		for (int i = 0; i < blocks.length; i++)
			all += blocks[i].remaining();
		long total = 0;
		while (total < all) {
			long bytes = channel.write(blocks, 0, blocks.length);
			total += bytes;
		}
		return total;
	}
	
	/**
	 * Writes from the matrix to an unstriped destination.
	 * @param channel
	 * @return
	 * @throws IOException
	 */
	public long writeStripes(GatheringByteChannel channel) throws IOException {
		return channel.write(stripes);
	}

	public void calculate(StripeCoder stripeCoder, int calcMask) {
		for (int s = 0; s < stripeBlocks.length; s++)
			stripeCoder.calculate(stripeBlocks[s], calcMask);
	}

}
