package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import warrenfalk.fuselaj.FilesystemException;

public class MeldFs {
	SourceFs[] sources;
	ExecutorService threadPool;

	public MeldFs() throws IOException {
		MeldFsProperties props = new MeldFsProperties();
		Path[] sources = props.getSources();
		this.sources = SourceFs.fromPaths(sources);
		threadPool = Executors.newCachedThreadPool();
	}

	// TODO: consider making this a private operation
	/** Runs a source operation against all selected sources concurrently, returning only when all are complete.
	 * A source is selected if the element at its position within the mask argument is not null
	 * @param mask
	 * @param operation
	 * @throws FilesystemException
	 */
	public void runMultiSourceOperation(Object[] mask, SourceOp operation) throws FilesystemException {
		final AtomicInteger sync = new AtomicInteger(sources.length);
		try {
			synchronized (sync) {
				for (int i = 0; i < sources.length; i++) {
					if (null == mask || mask[i] != null)
						threadPool.execute(new SourceOpRunner(operation, i, sources[i], sync));
					else
						sync.decrementAndGet();
				}
				if (sync.intValue() > 0)
					sync.wait();
			}
		}
		catch (InterruptedException e) {
			throw new FilesystemException(e);
		}
	}
	
	/** Runs a source operation against all sources concurrently, returning only when all are complete
	 * @param mask
	 * @param operation
	 * @throws FilesystemException
	 */
	public void runMultiSourceOperation(SourceOp operation) throws FilesystemException {
		runMultiSourceOperation(null, operation);
	}
	
	/** Return the real path representing the virtual path if on one device, or the real path to the most recently
	 * modified version of the file if on multiple devices
	 * @param vpath
	 * @return
	 * @throws InterruptedException
	 * @throws FilesystemException 
	 */
	public Path getRealPath(final Path vpath) throws FilesystemException {
		final Path[] files = new Path[sources.length];
		final long[] modTimes = new long[sources.length];
		getAllRealPaths(vpath, files, modTimes);
		return freshestFile(files, modTimes);
	}
	
	public void getAllRealPaths(final Path vpath, final Path[] rpaths, final long[] modTimes) throws FilesystemException {
		runMultiSourceOperation(new SourceOp() {
			@Override
			public void run(int index, SourceFs source) {
				Path sourceLoc = source.root.resolve(vpath);
				if (Files.exists(sourceLoc, LinkOption.NOFOLLOW_LINKS)) {
					try {
						if (modTimes != null)
							modTimes[index] = Files.getLastModifiedTime(sourceLoc, LinkOption.NOFOLLOW_LINKS).toMillis();
						rpaths[index] = sourceLoc;
					}
					catch (IOException ioe) {
						source.onIoException(ioe);
					}
				}
			}
		});
	}
	
	/**
	 * Returns the index of the freshest file.
	 * I.e. return x for the greatest modTimes[x] for which files[x] is not null, or -1 if all files are null.
	 * Lower values of x are favored in ties.
	 * @param rpaths
	 * @param modTimes
	 * @return
	 */
	public static int freshest(Path[] rpaths, long[] modTimes) {
		int result = -1;
		long max = Long.MIN_VALUE;
		for (int i = 0; i < rpaths.length; i++) {
			if (rpaths[i] == null)
				continue;
			long mod = modTimes[i];
			if (mod > max) {
				result = i;
				max = mod;
			}
		}
		return result;
	}
	
	/**
	 * Returns the real path of the freshest version of the file across all sources
	 * @param rpaths
	 * @param modTimes
	 * @return
	 */
	public static Path freshestFile(Path[] rpaths, long[] modTimes) {
		int i = freshest(rpaths, modTimes);
		if (i == -1)
			return null;
		return rpaths[i];
	}

	public int getSourceCount() {
		return sources.length;
	}

	public SourceFs getSource(int index) {
		return sources[index];
	}
	
}
