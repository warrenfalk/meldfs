package warrenfalk.meldfs;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import warrenfalk.fuselaj.DirBuffer;
import warrenfalk.fuselaj.Errno;
import warrenfalk.fuselaj.FileInfo;
import warrenfalk.fuselaj.Filesystem;
import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.fuselaj.Stat;
import warrenfalk.fuselaj.example.ExampleFs.FileHandle;

public class MeldFs extends Filesystem {
	SourceFs[] sources;
	String[] fuseArgs;
	ExecutorService threadPool;
	
	public MeldFs(File mountLoc, File[] sources, boolean debug, String options) {
		super(true);
		this.sources = SourceFs.fromFiles(sources);
		ArrayList<String> arglist = new ArrayList<String>();
		if (debug)
			arglist.add("-d");
		arglist.add(mountLoc.getAbsolutePath());
		if (options != null && options.length() > 0) {
			arglist.add("-o");
			arglist.add(options);
		}
		fuseArgs = arglist.toArray(new String[arglist.size()]);
		threadPool = Executors.newCachedThreadPool();
	}
	
	int run() {
		return run(fuseArgs);
	}

	/** Entry point for the MeldFs process
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO: Debugging code follows
		boolean debug = true;
		File mountPoint = new File("/dev/shm/test"); 
		File[] devices = new File[] {
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/0"),
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/1"),
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/2"),
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/3"),
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/4"),
				new File("/media/aacbd496-ae23-4be4-8f12-ffb733c1dbfa/test/5"),
		};
		//String options = "allow_other,use_ino,big_writes";
		String options = "allow_other,big_writes";
		for (File device : devices) {
			if (!device.isDirectory())
				device.mkdirs();
		}
		
		MeldFs fs = new MeldFs(mountPoint, devices, debug, options);
		int exitCode = fs.run();
		System.exit(exitCode);
	}
	
	@Override
	protected void getattr(String path, Stat stat) throws FilesystemException {
		try {
			// attempt to find entry with that name
			File file = getFile(path);
			if (file == null)
				throw new FilesystemException(Errno.NoSuchFileOrDirectory);
			os_stat(file.getAbsolutePath(), stat);
		}
		catch (InterruptedException ie) {
			throw new FilesystemException(Errno.InterruptedSystemCall);
		}
	}
	
	@Override
	protected void opendir(String path, FileInfo fi) throws FilesystemException {
		try {
			File dir = getFile(path);
			if (!dir.isDirectory())
				throw new FilesystemException(Errno.NotADirectory);
			FileHandle.open(fi, path);
		}
		catch (InterruptedException e) {
			throw new FilesystemException(Errno.InterruptedSystemCall);
		}
	}
	
	@Override
	protected void readdir(String path, DirBuffer dirBuffer, FileInfo fileInfo) throws FilesystemException {
		FileHandle fh = FileHandle.get(fileInfo.getFileHandle());
		path = (String)fh.data;
		// TODO: finish
	}
	
	@Override
	protected void releasedir(String path, FileInfo fi) throws FilesystemException {
		FileHandle.release(fi);
	}
	
	private File getDirs(final String path) throws InterruptedException {
		final File[] dirs = new File[sources.length];
		final AtomicInteger sync = new AtomicInteger(sources.length);
		for (int i = 0; i < sources.length; i++) {
			final int index = i;
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					SourceFs source = sources[index];
					File sourceLoc = new File(source.location, path);
					if (sourceLoc.isDirectory()) {
						dirs[index] = sourceLoc;
						modTimes[index] = sourceLoc.lastModified();
					}
					if (0 == sync.decrementAndGet()) {
						synchronized (sync) {
							sync.notify();
						}
					}
						
				}
			});
		}
		synchronized (sync) {
			sync.wait();
		}
		long max = 0L;
		File result = null;
		for (int i = 0; i < sources.length; i++) {
			File file = dirs[i];
			if (file == null)
				continue;
			long mod = modTimes[i];
			if (mod > max) {
				result = file;
				max = mod;
			}
		}
		return result;
	}
	
	private File getFile(final String path) throws InterruptedException {
		final File[] files = new File[sources.length];
		final long[] modTimes = new long[sources.length];
		final AtomicInteger sync = new AtomicInteger(sources.length);
		for (int i = 0; i < sources.length; i++) {
			final int index = i;
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					SourceFs source = sources[index];
					File sourceLoc = new File(source.location, path);
					if (sourceLoc.exists()) {
						files[index] = sourceLoc;
						modTimes[index] = sourceLoc.lastModified();
					}
					if (0 == sync.decrementAndGet()) {
						synchronized (sync) {
							sync.notify();
						}
					}
						
				}
			});
		}
		synchronized (sync) {
			sync.wait();
		}
		long max = 0L;
		File result = null;
		for (int i = 0; i < sources.length; i++) {
			File file = files[i];
			if (file == null)
				continue;
			long mod = modTimes[i];
			if (mod > max) {
				result = file;
				max = mod;
			}
		}
		return result;
	}
	
}
