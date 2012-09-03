package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class FileStriper {
	final int blockSize;
	final int dataSources;
	final int checksumSources;
	final int ringBufferSize;
	final StripeCoder stripeCoder;
	
	final static ExecutorService threadPool = Executors.newCachedThreadPool();

	public FileStriper(StripeCoder stripeCoder, int blockSize, int dataSources, int checksumSources, int ringBufferSize) {
		this.blockSize = blockSize;
		this.dataSources = dataSources;
		this.checksumSources = checksumSources;
		this.stripeCoder = stripeCoder;
		this.ringBufferSize = ringBufferSize;
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			printUsage();
			System.exit(1);
		}
		
		String action = null;
		String protocol = "R";
		int dataSize = -1;
		int checksumSize = -1;
		int blockSize = 512;
		int ringBufferSize = 3;
		String inputArg = null;
		ArrayList<String> outputArgs = new ArrayList<String>();
		try {
			for (int i = 0; i < args.length; i++) {
				String arg = args[i];
				if (arg.startsWith("-")) {
					if (arg.startsWith("-p"))
						protocol = arg.substring(2);
					else if (arg.startsWith("-d"))
						dataSize = parseIntArg("-d", arg.substring(2));
					else if (arg.startsWith("-c"))
						checksumSize = parseIntArg("-c", arg.substring(2));
					else if (arg.startsWith("-b"))
						blockSize = parseIntArg("-b", arg.substring(2));
					else if (arg.startsWith("-r"))
						ringBufferSize = parseIntArg("-r", arg.substring(2));
					else
						throw new IllegalArgumentException("Unknown switch: " + arg);
				}
				else {
					if (action == null)
						action = arg;
					else if (inputArg == null)
						inputArg = arg;
					else
						outputArgs.add(arg);
				}
			}
			// validate arguments
			
			if (!"R".equals(protocol)) {
				throw new IllegalArgumentException("Illegal protocol, " + protocol + ", specified");
			}
			
			if (checksumSize == -1 && dataSize == -1) {
				checksumSize = 2;
				dataSize = outputArgs.size() - checksumSize;
			}
			else if (checksumSize == -1) {
				checksumSize = outputArgs.size() - dataSize;
			}
			else if (dataSize == -1) {
				dataSize = outputArgs.size() - checksumSize;
			}
			if (outputArgs.size() != (dataSize + checksumSize)) {
				throw new IllegalArgumentException("number of outputs, " + outputArgs.size() + ", must match data outputs plus checksum outputs (" + dataSize + " + " + checksumSize + " = " + (dataSize + checksumSize) + ")");
			}
		}
		catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			printUsage();
			System.exit(2);
		}
		
		FileSystem fs = FileSystems.getDefault();
		Path inputPath = fs.getPath(inputArg);
		Path[] outputPaths = new Path[outputArgs.size()];
		for (int i = 0; i < outputArgs.size(); i++)
			outputPaths[i] = fs.getPath(outputArgs.get(i));
		
		FileChannel inputChannel = FileChannel.open(inputPath, StandardOpenOption.READ);
		FileChannel[] outputChannels = new FileChannel[outputPaths.length];
		for (int i = 0; i < outputChannels.length; i++)
			outputChannels[i] = FileChannel.open(outputPaths[i], StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		
		StripeCoder stripeCoder = null;

		if ("R".equals(protocol)) {
			ReedSolomonCodingDomain domain = new ReedSolomonCodingDomain(dataSize, checksumSize);
			final Coder coder = domain.getChecksumCoder();
			stripeCoder = new StripeCoder() {
				@Override
				public int calculate(ByteBuffer[] columns, int calcMask) {
					return coder.calculate(columns, calcMask);
				}
			};
		}
		
		FileStriper striper = new FileStriper(stripeCoder, blockSize, dataSize, checksumSize, ringBufferSize);
		striper.stripe(inputChannel, outputChannels);
	}
	
	private static int parseIntArg(String argswitch, String argval) {
		int radix = 10;
		if (argval.startsWith("0x")) {
			argval = argval.substring(2);
			radix = 16;
		}
		int value;
		try {
			value = Integer.parseInt(argval, radix);
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("Switch, \"" + argswitch + "\", must be a valid integer");
		}
		return value;
	}

	private static void printUsage() {
		System.out.println("meldfs stripe -pR -d# -c# -b# input output output output ...");
		System.out.println();
		System.out.println("  stripe options");
		System.out.println("     -pP  use protocol P where P is one of the following:");
		System.out.println("         R - Reed Solomon");
		System.out.println("     -d#  data outputs");
		System.out.println("          the first # outputs are data");
		System.out.println("          default: number of outputs minus checksum count");
		System.out.println("     -c#  checksum outputs");
		System.out.println("          the last # outputs are checksum");
		System.out.println("          default: 2");
		System.out.println("     -b#  block size");
		System.out.println("          default 512");
		System.out.println("     Note: if you specify both -d and -c, then the number of inputs");
		System.out.println("           must match the total between them.  If you specify only");
		System.out.println("           one, or neither, they will be calculated using defaults");
		System.out.println("           and the provided number of inputs");
		System.out.println("     ---------------------------------------------------");
		System.out.println("     extra stripe options");
		System.out.println("     -r#   stripe ring buffer size");
	}
	
	private <T> BlockingQueue<T> createQueue() {
		//return new ArrayBlockingQueue<T>(ringBufferSize);
		//return new LinkedBlockingDeque<T>(ringBufferSize);
		return new RingBuffer<T>(ringBufferSize);
	}
	
	interface BlockingQueue<T> {
		int takeAll(T[] buffer) throws InterruptedException;
		T take() throws InterruptedException;
		void put(T item) throws InterruptedException;
		void putAll(T[] buffer, int itemCount) throws InterruptedException;
	}
	
	static class RingBuffer<T> implements BlockingQueue<T> {
		final Semaphore readReady;
		final Semaphore writeReady;
		final Object[] items;
		int putCursor;
		int takeCursor;
		
		public RingBuffer(int capacity) {
			this.readReady = new Semaphore(0);
			this.writeReady = new Semaphore(capacity);
			this.items = new Object[capacity];
		}

		@Override
		public T take() throws InterruptedException {
			readReady.acquire();
			@SuppressWarnings("unchecked")
			T item = (T)items[takeCursor++];
			if (takeCursor == items.length)
				takeCursor = 0;
			writeReady.release();
			return item;
		}

		@SuppressWarnings("unchecked")
		@Override
		public int takeAll(T[] buffer) throws InterruptedException {
			readReady.acquire();
			int len = 1;
			len += readReady.drainPermits();
			for (int i = 0; i < len; i++) {
				buffer[i] = (T)items[takeCursor++];
				if (takeCursor == items.length)
					takeCursor = 0;
			}
			writeReady.release(len);
			return len;
		}

		@Override
		public void put(T item) throws InterruptedException {
			writeReady.acquire();
			items[putCursor++] = item;
			if (putCursor == items.length)
				putCursor = 0;
			readReady.release();
		}

		@Override
		public void putAll(T[] buffer, int itemCount) throws InterruptedException {
			while (itemCount > 0) {
				writeReady.acquire();
				int len = 1;
				while (len < itemCount && writeReady.tryAcquire())
					len++;
				itemCount -= len;
				for (int i = 0; i < len; i++) {
					items[putCursor++] = buffer[i];
					if (putCursor == items.length)
						putCursor = 0;
				}
				readReady.release(len);
			}
		}
		
	}
	
	private <T> BlockingQueue<T>[] createQueues(int count) {
		@SuppressWarnings("unchecked")
		BlockingQueue<T>[] queues = (BlockingQueue<T>[])new BlockingQueue<?>[count];
		for (int i = 0; i < count; i++)
			queues[i] = createQueue();
		return queues;
	}
	
	private static class StripeStatus {
		Throwable readException;
		Throwable[] writeExceptions;
		boolean canceled;
		
		boolean isCanceled() {
			return canceled;
		}
		
		void cancel() {
			canceled = true;
		}
	}

	public void stripe(final ScatteringByteChannel input, final GatheringByteChannel[] outputs) throws IOException {
		// verify there is one output per source
		if (outputs.length != dataSources + checksumSources)
			throw new IllegalArgumentException("tried to use a " + dataSources + "x" + checksumSources + " striper with " + outputs.length + " outputs");

		// we'll operate one stripe at a time
		// with one reading thread and X writing threads where X is the number of outputs
		// each thread will be fed with its own queue

		// create the reading and writing queues
		final BlockingQueue<Stripe> readReady = createQueue();
		final BlockingQueue<Stripe>[] writeReady = createQueues(dataSources + checksumSources);
		
		final StripeStatus status = new StripeStatus();
		status.writeExceptions = new Throwable[dataSources + checksumSources];
		
		// create the reader thread
		Thread reader = new Thread("Striper Reader") {
			public void run() {
				Stripe[] stripes = new Stripe[ringBufferSize];
				try {
					boolean eof;
					do {
						// get the next available stripe
						int stripeCount = readReady.takeAll(stripes);
						if (!status.isCanceled()) {
							// fill it with data from the input channel
							try {
								Stripe.fillAll(stripes, stripeCount, input);
							}
							catch (Throwable e) {
								status.readException = e;
								status.cancel();
							}
						}
						eof = false;
						for (int i = 0; i < stripeCount; i++)
							if (stripes[i].eof)
								eof = true;
						// send the stripe to all the writers
						for (int i = 0; i < writeReady.length; i++)
							writeReady[i].putAll(stripes, stripeCount);
					} while (!eof && !status.isCanceled());
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		
		// create the writer threads
		Thread[] writers = new Thread[writeReady.length];
		for (int i = 0; i < writers.length; i++) {
			// Prep the local variables for this column
			final int column = i;
			final GatheringByteChannel output = outputs[column];
			final BlockingQueue<Stripe> writeReadyQueue = writeReady[i];
			Thread writer = new Thread("Striper Writer [" + column + "]") {
				public void run() {
					try {
						Stripe[] stripes = new Stripe[ringBufferSize];
						while (true) {
							int stripeCount = writeReadyQueue.takeAll(stripes);
							if (status.isCanceled())
								break;
							try {
								Stripe.emptyAll(stripes, stripeCount, stripeCoder, column, output);
							}
							catch (Throwable e) {
								e.printStackTrace();
								status.writeExceptions[column] = e;
								status.cancel();
							}
							if (stripes[stripeCount - 1].eof)
								break;
							for (int i = 0; i < stripeCount; i++)
								if (stripes[i].emptyComplete())
									readReady.put(stripes[i]);
							if (status.isCanceled())
								break;
						}
					}
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			};
			writers[i] = writer;
		}

		// start the threads
		reader.start();
		for (int i = 0; i < writers.length; i++)
			writers[i].start();
		
		
		BufferPool bufferPool = new BufferPool(ringBufferSize * (dataSources + checksumSources) * blockSize);
		
		try {
			// create new stripes ready for reading, and queue them
			Stripe[] stripes = new Stripe[ringBufferSize];
			for (int i = 0; i < ringBufferSize; i++)
				stripes[i] = new Stripe(bufferPool, dataSources, checksumSources, blockSize);
			readReady.putAll(stripes, stripes.length);
		}
		catch (InterruptedException e) {
			return;
		}
		
		
		try {
			reader.join();
			for (int i = 0; i < writers.length; i++)
				writers[i].join();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		// throw any exceptions, starting with IO exceptions
		IOException ioexception = null;
		if (status.readException instanceof IOException) {
			ioexception = (IOException)status.readException;
		}
		else {
			for (Throwable e : status.writeExceptions) {
				if (e instanceof IOException) {
					if (ioexception == null)
						ioexception = (IOException)e;
					else if (ioexception != e)
						ioexception.addSuppressed(e);
				}
			}
		}
		
		if (ioexception != null)
			throw ioexception;

		// if non-IOExceptions exist, throw those
		Throwable throwable = null;
		if (status.readException != null) {
			throwable = status.readException;
		}
		else {
			for (Throwable e : status.writeExceptions) {
				if (e != null) {
					if (throwable == null)
						throwable = e;
					else if (throwable != e)
						throwable.addSuppressed(e);
				}
			}
		}
		
		if (throwable instanceof RuntimeException)
			throw (RuntimeException)throwable;
		else if (throwable instanceof Error)
			throw (Error)throwable;
		else if (throwable != null)
			throw new RuntimeException(throwable);
	}

	static class BufferPool {
		final ByteBuffer mother;
		int next;
		
		public BufferPool(int capacity) {
			mother = ByteBuffer.allocateDirect(capacity);
		}
		
		synchronized StripeBuffer getBuffer(int blockSize, int dataBlocks, int checksumBlocks) {
			int blocksPerStripe = dataBlocks + checksumBlocks;
			int stripeSize = blockSize * blocksPerStripe;
			mother.position(next);
			next += stripeSize;
			mother.limit(next);
			ByteBuffer stripe = mother.slice();
			ByteBuffer[] blocks = new ByteBuffer[blocksPerStripe];
			for (int i = 0; i < blocks.length; i++) {
				stripe.position(i * blockSize);
				stripe.limit((i + 1) * blockSize);
				blocks[i] = stripe.slice();
			}
			stripe.limit(dataBlocks * blockSize);
			stripe.position(0);
			stripe = stripe.slice();
			return new StripeBuffer(stripe, blocks);
		}
		
	}
	
	static final class StripeBuffer {
		final ByteBuffer stripe;
		final ByteBuffer[] blocks;
		
		public StripeBuffer(ByteBuffer stripe, ByteBuffer[] blocks) {
			this.stripe = stripe;
			this.blocks = blocks;
		}
	}
	
	private static class Stripe {
		final StripeBuffer stripeBuffer;
		final int dataCount;
		final AtomicInteger writersRemaining;
		boolean eof;
		
		public Stripe(BufferPool bufferPool, int dataCount, int checksumCount, int blockSize) {
			this.dataCount = dataCount;
			stripeBuffer = bufferPool.getBuffer(blockSize, dataCount, checksumCount);
			writersRemaining = new AtomicInteger(0);
		}

		/** signals that the empty for the current writer is complete, and returns true if all other writers are also complete */
		public boolean emptyComplete() {
			return writersRemaining.decrementAndGet() == 0;
		}

		public void fill(ReadableByteChannel channel) throws IOException {
			ByteBuffer stripe = stripeBuffer.stripe;
			ByteBuffer[] blocks = stripeBuffer.blocks;
			stripe.clear();
			long read;
			while (stripe.hasRemaining()) {
				read = channel.read(stripe);
				if (read == -1) {
					eof = true;
					break;
				}
			}
			stripe.flip();
			for (int i = 0; i < blocks.length; i++) {
				ByteBuffer block = blocks[i];
				block.position(0);
				int limit = Math.min(block.capacity(), stripe.remaining());
				block.limit(limit);
				stripe.position(stripe.position() + limit);
				assert (i >= dataCount || eof || block.remaining() == block.capacity());
			}
			// once full, prepare to queue the writers
			writersRemaining.set(blocks.length);
		}
		
		public static void fillAll(Stripe[] stripes, int stripeCount, ScatteringByteChannel input) throws IOException {
			// TODO: don't allocate here anymore
			ByteBuffer[] bb = new ByteBuffer[stripeCount];
			long remaining = 0;
			for (int i = 0; i < bb.length; i++) {
				stripes[i].stripeBuffer.stripe.clear();
				bb[i] = stripes[i].stripeBuffer.stripe;
				remaining += bb[i].remaining();
			}
			while (remaining > 0) {
				long bytes = input.read(bb);
				if (bytes == -1)
					break;
				remaining -= bytes;
			}
			for (int s = 0; s < stripeCount; s++) {
				ByteBuffer stripe = stripes[s].stripeBuffer.stripe;
				if (stripe.position() != stripe.limit())
					stripes[s].eof = true;
				stripe.flip();
				for (int i = 0; i < stripes[s].stripeBuffer.blocks.length; i++) {
					ByteBuffer block = stripes[s].stripeBuffer.blocks[i];
					block.position(0);
					int limit = Math.min(block.capacity(), stripe.remaining());
					block.limit(limit);
					stripe.position(stripe.position() + limit);
				}
				stripes[s].writersRemaining.set(stripes[s].stripeBuffer.blocks.length);
			}
		}

		/** calculate checksum number [index], where [index] is 0 for the first checksum column */
		private void calc(StripeCoder stripeCoder, int index) {
			int csindex = index + dataCount;
			int calcMask = 1 << (csindex);
			stripeCoder.calculate(stripeBuffer.blocks, calcMask);
		}
		
		public void empty(StripeCoder stripeCoder, int column, WritableByteChannel output) throws IOException {
			//if (stripeBuffer.stripe.position() > 0) {
				ByteBuffer buffer = stripeBuffer.blocks[column];
				if (column >= dataCount)
					calc(stripeCoder, column - dataCount);
				output.write(buffer);
				assert(buffer.hasRemaining() == false);
			//}
		}
		
		public static void emptyAll(Stripe[] stripes, int stripeCount, StripeCoder stripeCoder, int column, GatheringByteChannel output) throws IOException {
			int dataCount = stripes[0].dataCount;
			if (column >= dataCount) {
				for (int i = 0; i < stripeCount; i++)
					stripes[i].calc(stripeCoder, column - dataCount);
			}
			// TODO: don't allocate here anymore
			ByteBuffer[] bb = new ByteBuffer[stripeCount];
			for (int i = 0; i < bb.length; i++)
				bb[i] = stripes[i].stripeBuffer.blocks[column];
			output.write(bb);
		}
	}
	
}
