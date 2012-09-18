package warrenfalk.meldfs;

import java.util.concurrent.atomic.AtomicInteger;

import warrenfalk.fuselaj.FilesystemException;


public class SourceOpRunner implements Runnable {
	final int index;
	final SourceFs source;
	final AtomicInteger sync;
	final SourceOp operation;
	final FilesystemException[] errors;
	
	public SourceOpRunner(SourceOp operation, int index, SourceFs source, AtomicInteger sync, FilesystemException[] errors) {
		this.index = index;
		this.source = source;
		this.sync = sync;
		this.operation = operation;
		this.errors = errors;
	}
	
	@Override
	public void run() {
		try {
			operation.run(index, source);
		}
		catch (FilesystemException e) {
			errors[index] = e;
		}
		finally {
			if (0 == sync.decrementAndGet()) {
				synchronized (sync) {
					sync.notify();
				}
			}
		}
	}
}
