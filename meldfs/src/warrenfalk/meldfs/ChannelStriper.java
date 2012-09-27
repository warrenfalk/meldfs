package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ChannelStriper {
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

	public ChannelStriper(StripeCoder stripeCoder, int blockSize, int dataSources, int checksumSources, int ringBufferSize) {
		this.blockSize = blockSize;
		this.dataSources = dataSources;
		this.checksumSources = checksumSources;
		this.stripeCoder = stripeCoder;
		this.ringBufferSize = ringBufferSize;
		this.writeTime = new long[dataSources + checksumSources];
		this.calcTime = new AtomicLong();
		this.currentWriters = new AtomicInteger();
	}
	
	/** Parse an integer argument from the command line **/
	/** Retains the current status of the striping operation **/
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

	/** A stripe frame is passed to multiple threads for read/calc/write operations **/
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
	
	/** Simple fixed-length queue for stripe frames **/
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

	/** Stripe all data from input, putting result in outputs, return number of data bytes written (i.e. excluding checksum bytes written) **/
	public long stripe(final ScatteringByteChannel input, final GatheringByteChannel[] outputs) throws IOException, InterruptedException {
		final AtomicLong written = new AtomicLong();
		
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
								long size = frame.matrix.writeColumn(column, output);
								if (column < dataSources)
									written.addAndGet(size);
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
		
		return written.longValue();
	}
	
}
