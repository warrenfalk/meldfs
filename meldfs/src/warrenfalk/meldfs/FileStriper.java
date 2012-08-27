package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class FileStriper {
	final int blockSize;
	final int dataSources;
	final int checksumSources;
	final StripeCoder stripeCoder;
	
	final static ExecutorService threadPool = Executors.newCachedThreadPool();

	public FileStriper(StripeCoder stripeCoder, int blockSize, int dataSources, int checksumSources) {
		this.blockSize = blockSize;
		this.dataSources = dataSources;
		this.checksumSources = checksumSources;
		this.stripeCoder = stripeCoder;
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
		
		FileStriper striper = new FileStriper(stripeCoder, blockSize, dataSize, checksumSize);
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
	}
	
	private <T> BlockingQueue<T> createQueue() {
		return new ArrayBlockingQueue<T>(3);
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
		
		synchronized boolean isCanceled() {
			return canceled;
		}
		
		synchronized void cancel() {
			canceled = true;
		}
	}
	
	public void stripe(final ScatteringByteChannel input, final WritableByteChannel[] outputs) throws IOException {
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
				Stripe stripe = null;
				try {
					do {
						// get the next available stripe
						stripe = readReady.take();
						if (!status.isCanceled()) {
							// fill it with data from the input channel
							try {
								stripe.fill(input);
							}
							catch (Throwable e) {
								status.readException = e;
								status.cancel();
							}
						}
						// send the stripe to all the writers
						for (int i = 0; i < writeReady.length; i++)
							writeReady[i].put(stripe);
					} while (!stripe.eof && !status.isCanceled());
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
			final WritableByteChannel output = outputs[column];
			final BlockingQueue<Stripe> writeReadyQueue = writeReady[i];
			Thread writer = new Thread("Striper Writer [" + column + "]") {
				public void run() {
					try {
						while (true) {
							Stripe stripe = writeReadyQueue.take();
							if (status.isCanceled())
								break;
							try {
								stripe.empty(stripeCoder, column, output);
							}
							catch (Throwable e) {
								e.printStackTrace();
								status.writeExceptions[column] = e;
								status.cancel();
							}
							if (stripe.eof)
								break;
							if (stripe.emptyComplete())
								readReady.put(stripe);
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
		
		// create three new stripes ready for reading, and queue them
		try {
			for (int i = 0; i < 3; i++)
				readReady.put(new Stripe(dataSources, checksumSources, blockSize));
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
	
	private static ByteBuffer getBuffer(int blockSize) {
		// TODO: see if using allocate() is more efficient than allocateDirect()
		return ByteBuffer.allocate(blockSize);
	}
	
	private static class Stripe {
		final ByteBuffer[] buffers;
		final int dataCount;
		int size;
		int writersRemaining;
		boolean eof;
		
		public Stripe(int dataCount, int checksumCount, int blockSize) {
			this.dataCount = dataCount;
			int columnCount = dataCount + checksumCount;
			buffers = new ByteBuffer[columnCount];
			for (int i = 0; i < buffers.length; i++)
				buffers[i] = getBuffer(blockSize);
		}

		/** signals that the empty for the current writer is complete, and returns true if all other writers are also complete */
		public boolean emptyComplete() {
			synchronized (this) {
				writersRemaining--;
				return writersRemaining == 0;
			}
		}

		public void fill(ScatteringByteChannel channel) throws IOException {
			for (int i = 0; i < buffers.length; i++)
				buffers[i].clear();
			long read;
			// every stripe read except possibly the last must fill the data buffers
			// so calculate how much means full
			size = 0;
			int full = this.dataCount * buffers[0].capacity();
			while (size < full) {
				read = (int)channel.read(buffers, 0, dataCount);
				if (read == -1) {
					eof = true;
					break;
				}
				size += read;
			}
			for (int i = 0; i < buffers.length; i++) {
				assert (i >= dataCount || eof || buffers[i].hasRemaining() == false);
				buffers[i].flip();
			}
			// once full, prepare to queue the writers
			synchronized (this) {
				writersRemaining = buffers.length;
			}
		}
		
		/** calculate checksum number [index], where [index] is 0 for the first checksum column */
		private void calc(StripeCoder stripeCoder, int index) {
			int csindex = index + dataCount;
			int calcMask = 1 << (csindex);
			stripeCoder.calculate(buffers, calcMask);
			buffers[csindex].flip();
		}
		
		public void empty(StripeCoder stripeCoder, int column, WritableByteChannel output) throws IOException {
			if (size > 0) {
				ByteBuffer buffer = buffers[column];
				if (column >= dataCount)
					calc(stripeCoder, column - dataCount);
				output.write(buffer);
				assert(buffer.hasRemaining() == false);
			}
		}
	}
}
