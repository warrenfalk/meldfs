package warrenfalk.meldfs;

import java.io.File;
import java.util.ArrayList;

import warrenfalk.fuselaj.Filesystem;
import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.fuselaj.Stat;

public class MeldFs extends Filesystem {
	File[] sources;
	String[] fuseArgs;
	
	public MeldFs(File mountLoc, File[] sources, boolean debug, String options) {
		super(true);
		this.sources = sources;
		ArrayList<String> arglist = new ArrayList<String>();
		if (debug)
			arglist.add("-d");
		arglist.add(mountLoc.getAbsolutePath());
		if (options != null && options.length() > 0) {
			arglist.add("-o");
			arglist.add(options);
		}
		fuseArgs = arglist.toArray(new String[arglist.size()]);
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
		// attempt to find entry with that name
		File file = getFile(path);
		statFile(file, stat);
	}
	
	private File getFile(String path) {
		return new File("/dev/shm");
		// TODO: implement
	}
	
	private void statFile(File file, Stat stat) {
		int rval = os_stat(file.getPath(), stat);
		System.out.println("stat: ctime = " + stat.getCTime());
		/* TODO: implement the following, perhaps support this with JNI in fuselaj
		stat.putMode(inode.mode);
		stat.putLinkCount(1);
		stat.putSize(file.length());
		stat.putCTime(file.);
		stat.putModTime(file.lastModified());
		stat.putAccessTime(inode.atime);
		stat.putUserId(inode.uid);
		stat.putGroupId(inode.gid);
		*/
	}
}
