package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class FileStriper {
	final int blockSize;
	final int dataSources;
	final int checksumSources;
	final int ringBufferSize;
	final StripeCoder stripeCoder;
	long readTime;
	long[] writeTime;
	AtomicLong calcTime;
	AtomicInteger currentWriters;
	long writeStartTime;
	long totalWriteTime;
	
	final static ExecutorService threadPool = Executors.newCachedThreadPool();

	public FileStriper(StripeCoder stripeCoder, int blockSize, int dataSources, int checksumSources, int ringBufferSize) {
		this.blockSize = blockSize;
		this.dataSources = dataSources;
		this.checksumSources = checksumSources;
		this.stripeCoder = stripeCoder;
		this.ringBufferSize = ringBufferSize;
		this.writeTime = new long[dataSources + checksumSources];
		this.calcTime = new AtomicLong();
		this.currentWriters = new AtomicInteger();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length == 0) {
			printUsage();
			System.exit(1);
		}
		
		String action = null;
		String protocol = "R";
		int dataSize = -1;
		int checksumSize = -1;
		int blockSize = 512;
		int ringBufferSize = 32;
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
		
		long start = System.nanoTime();
		FileStriper striper = new FileStriper(stripeCoder, blockSize, dataSize, checksumSize, ringBufferSize);
		striper.stripe(inputChannel, outputChannels);
		long end = System.nanoTime();
		System.out.println("Wall Clock: " + format(end - start));
		System.out.println("      Read: " + format(striper.readTime));
		System.out.println("     Write: " + format(striper.totalWriteTime));
		for (int c = 0; c < outputChannels.length; c++)
			System.out.println("  Write[" + c + "]: " + format(striper.writeTime[c]));
		System.out.println("      Calc: " + format(striper.calcTime.longValue()));
	}
	
	private static String format(long nanoTime) {
		double seconds = (double)nanoTime / (double)1000000000.0;
		return "" + (Math.floor(seconds * 100.0) / 100.0);
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
	
	static class StripeFrame {
		final StripeMatrix matrix;
		final AtomicInteger columnLock;
		boolean eof;
		
		StripeFrame(int dataSources, int checksumSources, int blockSize) {
			matrix = new StripeMatrix(dataSources, checksumSources, blockSize);
			columnLock = new AtomicInteger();
			eof = false;
		}
		
		void resetLock(int columns) {
			columnLock.set(columns);
		}
		
		boolean releaseLock() {
			return 0 == columnLock.decrementAndGet();
		}
	}
	
	static class StripeFrameFifo {
		final StripeFrame[] buffer;
		final Semaphore takeReady;
		final Semaphore putReady;
		int takeCursor;
		int putCursor;
		
		StripeFrameFifo(int capacity) {
			buffer = new StripeFrame[capacity];
			takeReady = new Semaphore(0, false);
			putReady = new Semaphore(capacity, false);
			takeCursor = 0;
			putCursor = 0;
		}
		
		StripeFrame take() throws InterruptedException {
			takeReady.acquire();
			StripeFrame item = buffer[takeCursor];
			buffer[takeCursor] = null;
			takeCursor++;
			if (takeCursor == buffer.length)
				takeCursor = 0;
			putReady.release();
			return item;
		}
		
		void put(StripeFrame item) throws InterruptedException {
			putReady.acquire();
			buffer[putCursor++] = item;
			if (putCursor == buffer.length)
				putCursor = 0;
			takeReady.release();
		}
	}

	public void stripe(final ScatteringByteChannel input, final GatheringByteChannel[] outputs) throws IOException, InterruptedException {
		// verify there is one output per source
		if (outputs.length != dataSources + checksumSources)
			throw new IllegalArgumentException("tried to use a " + dataSources + "x" + checksumSources + " striper with " + outputs.length + " outputs");

		// we'll operate one stripe at a time
		// with one reading thread and X writing threads where X is the number of outputs
		// each thread will be fed with its own queue

		final StripeStatus status = new StripeStatus();
		status.writeExceptions = new Throwable[dataSources + checksumSources];
		
		final StripeFrameFifo readQueue = new StripeFrameFifo(ringBufferSize);
		final StripeFrameFifo[] writeQueues = new StripeFrameFifo[dataSources + checksumSources];
		for (int c = 0; c < writeQueues.length; c++)
			writeQueues[c] = new StripeFrameFifo(ringBufferSize);
		
		for (int i = 0; i < ringBufferSize; i++)
			readQueue.put(new StripeFrame(dataSources, checksumSources, blockSize));
		
		// create the writer threads
		Thread[] writers = new Thread[dataSources + checksumSources];
		for (int i = 0; i < writers.length; i++) {
			// Prep the local variables for this column
			final int column = i;
			final GatheringByteChannel output = outputs[column];
			final StripeFrameFifo writeQueue = writeQueues[i];
			Thread writer = new Thread("Striper Writer [" + column + "]") {
				public void run() {
					long start, end;
					try {
						for (;;) {
							if (status.isCanceled())
								return;
							StripeFrame frame = writeQueue.take();
							try {
								if (column >= dataSources) {
									start = System.nanoTime();
									frame.matrix.calculate(stripeCoder, 1 << column);
									end = System.nanoTime();
									calcTime.addAndGet(end - start);
								}
								int x = currentWriters.incrementAndGet();
								start = System.nanoTime();
								frame.matrix.writeColumn(column, output);
								end = System.nanoTime();
								if (x == 1)
									writeStartTime = start;
								if (0 == currentWriters.decrementAndGet())
									totalWriteTime += (end - writeStartTime);
								writeTime[column] += (end - start);
								if (frame.eof)
									break;
							}
							finally {
								if (frame.releaseLock())
									readQueue.put(frame);
							}
						}
					}
					catch (Throwable e) {
						e.printStackTrace();
						status.cancel();
					}
				}
			};
			writers[i] = writer;
		}

		// start the threads
		for (int i = 0; i < writers.length; i++)
			writers[i].start();

		for (;;) {
			StripeFrame frame = readQueue.take();
			long start = System.nanoTime();
			long size = frame.matrix.readStripes(input);
			long end = System.nanoTime();
			readTime += (end - start);
			if (size < frame.matrix.getTotalDataSize())
				frame.eof = true;
			frame.resetLock(writeQueues.length);
			for (int i = 0; i < writeQueues.length; i++)
				writeQueues[i].put(frame);
			if (frame.eof)
				break;
		}
		
		for (int i = 0; i < writers.length; i++)
			writers[i].join();

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
	
}
