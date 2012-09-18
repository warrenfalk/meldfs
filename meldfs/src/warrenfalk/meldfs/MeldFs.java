package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import warrenfalk.fuselaj.Errno;
import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.fuselaj.FuselajFs;

public class MeldFs {
	final Path rootPath = FileSystems.getDefault().getPath(".").normalize();
	private SourceFs[] sources;
	private ExecutorService threadPool;
	ThreadLocal<FilesystemException[]> _exceptions = new ThreadLocal<FilesystemException[]>();

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
	public FilesystemException[] runMultiSourceOperation(Object[] mask, SourceOp operation) throws FilesystemException {
		final AtomicInteger sync = new AtomicInteger(sources.length);
		// get the filesystem error holder
		FilesystemException[] fserrs = _exceptions.get();
		if (fserrs == null || fserrs.length != sources.length)
			_exceptions.set(fserrs = new FilesystemException[sources.length]);
		for (int i = 0; i < fserrs.length; i++)
			fserrs[i] = null;

		// now run the operations
		try {
			synchronized (sync) {
				for (int i = 0; i < sources.length; i++) {
					if (null == mask || mask[i] != null)
						threadPool.execute(new SourceOpRunner(operation, i, sources[i], sync, fserrs));
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
		
		// check for errors
		int errCount = 0;
		for (int i = 0; i < fserrs.length; i++)
			if (fserrs[i] != null)
				errCount++;
		
		// fashion an error array if necessary
		FilesystemException[] rval = null;
		if (errCount > 0) {
			rval = new FilesystemException[errCount];
			int j = 0;
			for (int i = 0; i < fserrs.length; i++)
				if (fserrs[i] != null)
					rval[j++] = fserrs[i]; 
		}
		
		return rval;
	}
	
	/** Runs a source operation against all sources concurrently, returning only when all are complete
	 * @param mask
	 * @param operation
	 * @return 
	 * @throws FilesystemException
	 */
	public FilesystemException[] runMultiSourceOperation(SourceOp operation) throws FilesystemException {
		return runMultiSourceOperation(null, operation);
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

	public FileChannel create(final Path path, boolean failIfExists, Set<? extends OpenOption> openOptions) throws FilesystemException {
		// when creating a file, first find the existing file if any
		// if it exists, and if this is a create_new, then fail, otherwise overwrite that one
		// if it doesn't exist, get the youngest parent and create one there
		final Path parent = parentOf(path);
		final Path[] files = new Path[sources.length];
		final Path[] parents = new Path[sources.length];
		final long[] modTimes = new long[sources.length];
		runMultiSourceOperation(new SourceOp() {
			public void run(int index, SourceFs source) {
				Path sourceLoc = source.root.resolve(path);
				if (Files.exists(sourceLoc)) {
					try {
						modTimes[index] = Files.getLastModifiedTime(sourceLoc).toMillis();
						files[index] = sourceLoc;
					}
					catch (IOException ioe) {
						source.onIoException(ioe);
					}
				}
				sourceLoc = source.root.resolve(parent);
				if (Files.exists(sourceLoc)) {
					try {
						modTimes[index] = Files.getLastModifiedTime(sourceLoc).toMillis();
						parents[index] = sourceLoc;
					}
					catch (IOException ioe) {
						source.onIoException(ioe);
					}
				}
			}
		});

		// find the youngest existing file (if any), first
		Path realPath = freshestFile(files, modTimes);
		if (realPath != null) {
			if (failIfExists)
				throw new FilesystemException(Errno.FileExists);
		}
		else {
			// no existing file, find the youngest parent and create the file there
			// TODO: if this drive is nearing capacity, find a new drive
			Path openDir = MeldFs.freshestFile(parents, modTimes);
			if (openDir == null)
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
			realPath = openDir.resolve(path.getFileName());
		}
		try {
			FileChannel channel = FileChannel.open(realPath, openOptions);
			return channel;
		}
		catch (IOException ioe) {
			throw new FilesystemException(ioe);
		}
	}
	
	/** Find the parent path of the given path.
	 * Note: this is distinct from path.getParent() in that it returns the rootPath instead of null
	 * @param path
	 * @return the parent of path
	 */
	Path parentOf(Path path) {
		Path parent = path.getParent();
		if (parent == null)
			parent = rootPath;
		return parent;
	}

	/** Delete file at virtual path <code>vpath</code> */
	public void rm(final Path vpath) throws FilesystemException {
		final AtomicInteger deleted = new AtomicInteger(0);
		final AtomicInteger found = new AtomicInteger(0);
		runMultiSourceOperation(new SourceOp() {
			@Override
			public void run(int index, SourceFs source) {
				Path sourceLoc = source.root.resolve(vpath);
				if (Files.exists(sourceLoc, LinkOption.NOFOLLOW_LINKS)) {
					found.incrementAndGet();
					try {
						Files.delete(sourceLoc);
						deleted.incrementAndGet();
					}
					catch (IOException e) {
						source.onIoException(e);
					}
				}
			}
		});
		if (found.intValue() == 0)
			throw new FilesystemException(Errno.NoSuchFileOrDirectory);
		// TODO: try to throw the actual error that resulted
		if (deleted.intValue() < found.intValue())
			throw new FilesystemException(Errno.IOError);
	}

	public void rmdir(final Path path) throws FilesystemException {
		final AtomicInteger found = new AtomicInteger(0);
		final AtomicInteger deleted = new AtomicInteger(0);
		FilesystemException[] errors = runMultiSourceOperation(new SourceOp() {
			@Override
			public void run(int index, SourceFs source) throws FilesystemException {
				Path sourceLoc = source.root.resolve(path);
				if (Files.exists(sourceLoc)) {
					found.incrementAndGet();
					boolean success = false;
					try {
						FuselajFs.os_rmdir(sourceLoc);
						deleted.incrementAndGet();
						success = true;
					}
					finally {
						if (!success) {
							// TODO: figure out what to do here, this should have some sort of transactional capability
							// We've tried to remove one of the instances of the directory, but at least one failed for some reason
						}
					}
				}
			}
		});
		if (found.intValue() == 0)
			throw new FilesystemException(Errno.NoSuchFileOrDirectory);

		if (found.intValue() != deleted.intValue()) {
			if (errors.length > 0)
				throw errors[0];
			throw new FilesystemException(Errno.IOError);
		}
	}
	
	
}
