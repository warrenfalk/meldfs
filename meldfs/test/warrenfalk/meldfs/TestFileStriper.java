package warrenfalk.meldfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;

import org.junit.Test;

public class TestFileStriper {

	@Test
	public void testMultiByteSource() throws IOException {
		byte[] source = createSource(2);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}
	
	@Test
	public void testSingleBlockSource() throws IOException {
		byte[] source = createSource(512);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}	
		
	@Test
	public void testSingleBlockSingleByteSource() throws IOException {
		byte[] source = createSource(513);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}	
		
	@Test
	public void testMultiStripeMultiByteSource() throws IOException {
		byte[] source = createSource(10 * 512 + 5);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}	
		
	@Test
	public void testMultiStripeSingleBlockSource() throws IOException {
		byte[] source = createSource(11 * 512);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}
		
	@Test
	public void testMultiStripeSingleBlockSingleByteSource() throws IOException {
		byte[] source = createSource(11 * 512 + 5);
		StripeTest stripeTest = new StripeTest(source, 512, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	@Test
	public void testTinyBlock() throws IOException {
		int blockSize = 3;
		byte[] source = createSource(11 * blockSize + 5);
		StripeTest stripeTest = new StripeTest(source, blockSize, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	@Test
	public void testLargeBlock() throws IOException {
		int blockSize = 4096;
		byte[] source = createSource(11 * blockSize + 5);
		StripeTest stripeTest = new StripeTest(source, blockSize, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	@Test
	public void testLongStripe() throws IOException {
		int blockSize = 16;
		byte[] source = createSource(24 * blockSize + 5);
		StripeTest stripeTest = new StripeTest(source, blockSize, 20, 3);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	@Test
	public void testShortStripe() throws IOException {
		int blockSize = 16;
		byte[] source = createSource(4 * blockSize + 5);
		StripeTest stripeTest = new StripeTest(source, blockSize, 2, 1);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	@Test
	public void testHundredStripeSource() throws IOException {
		int blockSize = 16;
		byte[] source = createSource(700 * blockSize + 5);
		StripeTest stripeTest = new StripeTest(source, blockSize, 5, 2);
		FileStriper striper = stripeTest.createStriper();
		striper.stripe(stripeTest.input, stripeTest.outputs);
	}

	private byte[] createSource(int length) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			int val = 'a' + (i % 26);
			bytes[i] = (byte)(val & 0xFF);
		}
		return bytes;
	}

	static class StripeTest {
		final byte[] source;
		final int blockSize;
		final int dataCount;
		final int checksumCount;
		final StripeCoder coder;
		final ScatteringByteChannel input;
		final WritableByteChannel[] outputs;
		
		StripeTest(final byte[] bytes, final int blockSize, final int dataCount, final int checksumCount) {
			this.source = bytes;
			this.blockSize = blockSize;
			this.dataCount = dataCount;
			this.checksumCount = checksumCount;
			
			coder = new StripeCoder() {
				@Override
				public int calculate(ByteBuffer[] columns, int calcMask) {
					// the checksum block must be as large as the
					// largest data block
					int csSize = 0;
					for (int i = 0; i < dataCount; i++)
						if (csSize < columns[i].limit())
							csSize = columns[i].limit();
					
					int result = 0;
					
					// do actual calculation
					for (int c = 0; c < checksumCount; c++) {
						int i = c + dataCount;
						if (0 == (calcMask & (1 << i)))
							continue;
						ByteBuffer buffer = columns[i];
						// move the pointer
						buffer.limit(csSize);
						for (int position = 0; position < csSize; position++) {
							int value = c + dataCount;
							for (int d = 0; d < dataCount; d++) {
								ByteBuffer dataColumn = columns[d];
								value = hash(value, (position < dataColumn.limit() ? dataColumn.get(position) : 0) & 0xFF);
							}
							buffer.put(position, (byte)(value & 0xFF));
						}
						buffer.position(csSize);
						buffer.flip();
						result += csSize;
					}
					
					return result;
				}
			};
			
			input = new MemoryChannel(bytes);
			
			outputs = new WritableByteChannel[dataCount + checksumCount];
			for (int i = 0; i < outputs.length; i++) {
				final int column = i;
				outputs[i] = new WritableByteChannel() {
					long position = 0;
					boolean open;

					@Override
					public boolean isOpen() {
						return open;
					}

					@Override
					public void close() throws IOException {
						open = false;
					}

					@Override
					public int write(ByteBuffer src) throws IOException {
						int len = 0;
						while (src.hasRemaining()) {
							byte b = src.get();
							writeByte(column, position, b);
							position++;
							len++;
						}
						return len;
					}
				};
			}
		}
		
		public FileStriper createStriper() {
			return new FileStriper(coder, blockSize, dataCount, checksumCount);			
		}

		void writeByte(int column, long position, byte b) {
			byte expected = getExpectedByteAt("write", column, position);
			byte actual = b;
			assertEquals("stripe byte written to position " + position + " of column " + column, expected, actual);
		}
		
		private byte getDataByteAt(int column, long position) {
			assert column < dataCount;
			long bpos = position % blockSize;
			long cblock = position / blockSize;
			long block = (cblock * dataCount) + column;
			long offset = block * blockSize + bpos;
			if (offset >= source.length)
				return 0;
			return source[(int)offset];
		}
		
		private byte getExpectedByteAt(String forOperation, int column, long position) {
			long bpos = position % blockSize;
			long cblock = position / blockSize;
			if (column < dataCount) {
				long block = (cblock * dataCount) + column;
				long offset = block * blockSize + bpos;
				if (offset > source.length)
					fail("attempt " + forOperation + " at column " + column + " at position " + position + " which is offset " + offset + " which is not in the source");
				return source[(int)offset];
			}
			else {
				// check range
				// TODO: move some of this calculation into constructor
				int completeSourceBlocks = source.length / blockSize;
				int minBlockPerColumn = completeSourceBlocks / dataCount;
				if (cblock > minBlockPerColumn) {
					fail("attempt " + forOperation + " at column " + column + " at block " + cblock + " which is not valid for that checksum column");
				}
				else if (cblock == minBlockPerColumn) {
					long eventStripeSize = (long)minBlockPerColumn * dataCount * (long)blockSize;
					int lastStripeExcess = (int)((long)source.length - eventStripeSize);
					int lastChecksumBlockSize = Math.min(lastStripeExcess, blockSize);
					if (bpos >= lastChecksumBlockSize)
						fail("attempt " + forOperation + " at column " + column + " at position " + position + " which is not valid for that checksum column");
				}
				
				int hash = column;
				for (int i = 0; i < dataCount; i++)
					hash = hash(hash, (int)getDataByteAt(i, position));
				return (byte)(hash & 0xFF);
			}
		}
		
		private int hash(int current, int next) {
			long k = current;
			long n = 2654435761L;
			n *= next;
			return (int)((k + n) & 0x7FFFFFFF);
		}
	}
	
	static class MemoryChannel implements ScatteringByteChannel {
		final ByteBuffer bb;
		boolean open;
		
		public MemoryChannel(byte[] bytes) {
			bb = ByteBuffer.wrap(bytes);
		}
		
		@Override
		public boolean isOpen() {
			return open;
		}
		
		@Override
		public void close() throws IOException {
			open = false;
		}
		
		@Override
		public int read(ByteBuffer dst) throws IOException {
			ByteBuffer src = bb.slice();
			if (!src.hasRemaining())
				return -1;
			int limit = Math.min(src.remaining(), dst.remaining());
			src.limit(limit);
			dst.put(src);
			bb.position(bb.position() + limit);
			return limit;
		}
		
		@Override
		public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
			long totlen = 0;
			for (int i = offset; i < (offset + length); i++) {
				int seglen = read(dsts[i]);
				if (seglen == -1)
					return (totlen == 0) ? -1 : totlen;
				totlen += seglen;
			}
			return totlen;
		}
		
		@Override
		public long read(ByteBuffer[] dsts) throws IOException {
			return read(dsts, 0, dsts.length);
		}
	}

}
