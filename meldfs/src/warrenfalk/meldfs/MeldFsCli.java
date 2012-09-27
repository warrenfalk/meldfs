package warrenfalk.meldfs;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import warrenfalk.fuselaj.FilesystemException;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class MeldFsCli {

	public static void main(String[] args) throws Throwable {
		Iterator<String> argList = Arrays.asList(args).iterator();
		
		String arg = argList.hasNext() ? argList.next() : null;
		
		if (arg == null) {
			printCommandList();
			return;
		}
		
		if (arg.startsWith("-")) {
			System.err.println("Invalid switch before command: " + arg);
			printCommandList();
			return;
		}
		
		// Try to find a public static method taking an argument iterator as a parameter, a boolean parameter and returning a string
		String command = arg;
		Method commandMethod = null;
		for (Method method : MeldFsCli.class.getMethods()) {
			int modifiers = method.getModifiers();
			if (!Modifier.isStatic(modifiers)) 
				continue;
			if (!method.getName().equals(command))
				continue;
			Class<?>[] params = method.getParameterTypes();
			if (params.length != 1)
				continue;
			if (params[0] != Iterator.class)
				continue;
			Command cmdAnnotation = method.getAnnotation(Command.class);
			if (cmdAnnotation == null)
				continue;
			commandMethod = method;
			break;
		}
		
		if (commandMethod == null) {
			System.err.println("Unrecognized command: " + command);
			printCommandList();
			return;
		}
		
		try {
			Object rval = commandMethod.invoke(null, argList);
		
			if (rval instanceof Integer) {
				System.exit(((Integer)rval).intValue());
			}
		}
		catch (InvocationTargetException ite) {
			throw ite.getCause();
		}
	}

	private static void printCommandList() {
		for (Method method : MeldFsCli.class.getMethods()) {
			int modifiers = method.getModifiers();
			if (!Modifier.isStatic(modifiers)) 
				continue;
			Class<?>[] params = method.getParameterTypes();
			if (params.length != 1)
				continue;
			if (params[0] != Iterator.class)
				continue;
			Command cmdAnnotation = method.getAnnotation(Command.class);
			if (cmdAnnotation == null)
				continue;
			System.out.println(method.getName() + "   " + cmdAnnotation.value());
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	public @interface Command {
		String value();
	}

	@Command("list the files in a meldfs filesystem")
	public static int ls(Iterator<String> args) throws IOException {
		// variables
		ArrayList<Path> vpathList = new ArrayList<>();
		boolean help = false;
		boolean all = false;
		// parse args into variables
		while (args.hasNext()) {
			String arg = args.next();
			if ("--help".equals(arg) || "-h".equals(arg)) {
				help = true;
				break;
			}
			if (arg.startsWith("-")) {
				if (!arg.startsWith("--")) {
					for (int i = 1; i < arg.length(); i++) {
						char sw = arg.charAt(i);
						switch (sw) {
						case 'a':
							all = true;
							break;
						default:
							System.err.println("Unrecognized switch: -" + sw);
							return 1;
						}
					}
					continue;
				}
				else {
					System.err.println("Unrecognized switch: " + arg);
					return 1;
				}
			}
			vpathList.add(FileSystems.getDefault().getPath(arg));
		}
		// give help if asked
		if (help) {
			System.out.println("Usage:");
			System.out.println("meldfs ls <vpath>[ <vpath>...]");
			return 1;
		}
		// run the command
		MeldFs meldfs = new MeldFs();
		if (vpathList.size() == 0)
			vpathList.add(meldfs.rootPath);
		for (Path vpath : vpathList) {
			vpath = sanitize(meldfs, vpath);
			if (vpathList.size() > 1)
				System.out.println(vpath + ":");
			try {
				Set<String> entrySet = meldfs.ls(vpath);
				if (all) {
					entrySet.add(".");
					entrySet.add("..");
				}
				for (Iterator<String> i = entrySet.iterator(); i.hasNext(); ) {
					String entry = i.next();
					if (!all && entry.startsWith("."))
						i.remove();
				}
				String[] entries = entrySet.toArray(new String[entrySet.size()]);
				Arrays.sort(entries);
				//System.out.println("total " + entries.size());
				for (String entry : entries) {
					System.out.println(entry);
				}
			}
			catch (FilesystemException fse) {
				System.out.println("Cannot access " + vpath + ": " + fse.getMessage());
			}
			System.out.println();
		}
		return 0;
	}
	
	@Command("automatically spread a file across the source filesystems redundantly")
	public static int autostripe(Iterator<String> args) throws IOException, InterruptedException {
		// variables
		ArrayList<Path> vpathList = new ArrayList<>();
		boolean help = false;
		int redundancy = 2;
		boolean verbose = false;
		// parse args into variables
		while (args.hasNext()) {
			String arg = args.next();
			if ("--help".equals(arg) || "-h".equals(arg)) {
				help = true;
				break;
			}
			if (arg.startsWith("-")) {
				if (!arg.startsWith("--")) {
					for (int i = 1; i < arg.length(); i++) {
						char sw = arg.charAt(i);
						switch (sw) {
						case 'v':
							verbose = true;
							break;
						default:
							System.err.println("Unrecognized switch: -" + sw);
							return 1;
						}
					}
					continue;
				}
				else {
					if ("--redundancy".startsWith(arg)) {
						redundancy = Integer.parseInt(args.next());
					}
					else {
						System.err.println("Unrecognized switch: " + arg);
						return 1;
					}
				}
			}
			vpathList.add(FileSystems.getDefault().getPath(arg));
		}
		// give help if asked
		if (help) {
			System.out.println("Usage:");
			System.out.println("meldfs autostripe [-v] [--redundancy #] <vpath> [<vpath>...]");
			return 1;
		}
		// run the command
		MeldFs meldfs = new MeldFs();
		for (Path vpath : vpathList) {
			vpath = sanitize(meldfs, vpath);
			if (verbose)
				System.out.println("Auto Striping \"" + vpath + "\":");
			AutoStriperFactory striperFactory = new AutoStriperFactory(meldfs, redundancy, 4096);
			autostripe(meldfs, striperFactory, vpath, redundancy, verbose);
		}
		
		return 0;
	}
	
	// TODO: consider moving to standalone class
	private static class AutoStriperFactory {
		final MeldFs meldfs;
		final int redundancy;
		final int blockSize;
		final LinkedList<AutoStriper> stripers;
		SourceFs[] sources;

		public AutoStriperFactory(MeldFs meldfs, int redundancy, int blockSize) {
			this.meldfs = meldfs;
			this.redundancy = redundancy;
			this.blockSize = blockSize;
			this.stripers = new LinkedList<AutoStriper>();
		}
		
		public AutoStriper getStriper() {
			// TODO: determine which sources have adequate space
			int sourceCount = meldfs.getSourceCount();
			int dataCount = sourceCount - redundancy;
			int checksumCount = redundancy;
			sources = new SourceFs[sourceCount];
			for (int i = 0; i < sourceCount; i++)
				sources[i] = meldfs.getSource(i);
			for (AutoStriper striper : stripers) {
				if (striper.matches(sources, dataCount, checksumCount))
					return striper;
			}
			AutoStriper striper = new AutoStriper(meldfs, sources, blockSize, dataCount, checksumCount);
			stripers.addFirst(striper);
			return striper;
		}
	}
	
	private static class AutoStriper {
		final MeldFs meldfs;
		final SourceFs[] sources;
		final int dataSize;
		final int checksumSize;
		final int blockSize;
		final ChannelStriper striper;

		public AutoStriper(MeldFs meldfs, SourceFs[] sources, int blockSize, int dataSize, int checksumSize) {
			this.meldfs = meldfs;
			this.dataSize = dataSize;
			this.checksumSize = checksumSize;
			this.sources = sources;
			this.blockSize = blockSize;
			final ReedSolomonCodingDomain rsdomain = new ReedSolomonCodingDomain(dataSize, checksumSize);
			StripeCoder coder = new StripeCoder() {
				Coder coder = rsdomain.getChecksumCoder();
				@Override
				public int calculate(ByteBuffer[] columns, int calcMask) {
					return coder.calculate(columns, calcMask);
				}
			};
			this.striper = new ChannelStriper(coder, blockSize, dataSize, checksumSize, 32);
		}
		
		boolean matches(SourceFs[] sources, int dataCount, int checksumCount) {
			if (dataCount != this.dataSize)
				return false;
			if (checksumCount != this.checksumSize)
				return false;
			if (sources.length != this.sources.length)
				return false;
			for (int i = 0; i < sources.length; i++)
				if (!this.sources[i].equals(sources[i]))
					return false;
			return true;
		}
		
		static final char[] hexchars = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		static String toHex(long number) {
			if (number == 0)
				return "0";
			char[] out = new char[16];
			int i = out.length;
			while (number != 0) {
				int nibble = (int)(number & 0xF);
				out[--i] = hexchars[nibble];
				number >>= 4;
			}
			return new String(out, i, out.length - i);
		}

		public long stripe(Path vpath) throws IOException, InterruptedException, FilesystemException {
			int columns = dataSize + checksumSize;
			long size = -1;
			int[] shuffled = new int[columns];
			for (int i = 0; i < columns; i++)
				shuffled[i] = i;
			int hash = vpath.getFileName().toString().hashCode();
			hash ^= (hash >>> 20) ^ (hash >>> 12);
			hash = hash ^ (hash >>> 7) ^ (hash >>> 4);
			hash = hash & Integer.MAX_VALUE;
			while (hash != 0) {
				int other = (hash % (columns - 1)) + 1; 
				int q = shuffled[0];
				shuffled[0] = shuffled[other];
				shuffled[other] = q;
				hash /= (columns - 1);
			}
			Path[] tempPaths = new Path[columns];
			FileChannel[] outputs = new FileChannel[columns];
			boolean verified = false;
			String name = vpath.getFileName().toString();
			Path rpath = null;
			try (ScatteringByteChannel input = meldfs.open(vpath, StandardOpenOption.READ)) {
				// create output channels
				for (int i = 0; i < columns; i++) {
					String tempName = createTempName(name);
					SourceFs source = sources[shuffled[i]];
					Path parent = source.root.resolve(".stripe").resolve(vpath).getParent();
					tempPaths[i] = parent.resolve(tempName);
					try {
						outputs[i] = FileChannel.open(tempPaths[i], StandardOpenOption.CREATE, StandardOpenOption.WRITE);
					}
					catch (IOException e) {
						source.handleWriteException(e);
						throw e;
					}
				}
				// do the striping
				size = striper.stripe(input, outputs);
				// close the output channels
				for (int i = 0; i < columns; i++) {
					outputs[i].close();
					outputs[i] = null;
				}
				// TODO: make this optional and vary the level of verification to 0. none, 1. data-only, 2. various degraded scenarios
				rpath = meldfs.getRealPath(vpath);
				verified = verify(size, tempPaths, rpath);
				if (!verified)
					throw new RuntimeException("Stripe verification failed");
			}
			finally {
				for (int i = 0; i < columns; i++) {
					try {
						if (outputs[i] != null)
							outputs[i].close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
				if (verified) {
					// commit the change
					for (int i = 0; i < columns; i++) {
						String stripedName = createStripedName(name, size, i);
						Path outPath = tempPaths[i].getParent();
						Files.move(tempPaths[i], outPath.resolve(stripedName), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
						tempPaths[i] = null;
					}
					// TODO: delete the original
					Files.delete(rpath);
				}
				else {
					size = -1;
				}
				for (int i = 0; i < columns; i++) {
					try {
						if (tempPaths[i] != null && Files.exists(tempPaths[i], LinkOption.NOFOLLOW_LINKS))
							Files.delete(tempPaths[i]);
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			return size;
		}

		boolean verify(long size, Path[] tempPaths, Path rpath) throws IOException, FilesystemException {
			// verify everything
			int columns = dataSize + checksumSize;
			long validMask = -1;
			byte[] sha1Striped, sha1Original;
			ScatteringByteChannel[] inputs = new ScatteringByteChannel[columns];
			try {
				for (int i = 0; i < inputs.length; i++)
					inputs[i] = FileChannel.open(tempPaths[i], StandardOpenOption.READ);
				try (StripeChannel striped = new StripeChannel(meldfs.threadPool, size, dataSize, blockSize, inputs, validMask)) {
					sha1Striped = sha1(striped);
				}
			}
			finally {
				for (int i = 0; i < inputs.length; i++)
					if (inputs[i] != null)
						inputs[i].close();
			}
			try (ScatteringByteChannel original = FileChannel.open(rpath, StandardOpenOption.READ)) {
				sha1Original = sha1(original);
			}
			return areEqual(sha1Striped, sha1Original);
		}
		
		public String createTempName(String realName) {
			return ".mfs~" + realName;
		}
		
		public String createStripedName(String realName, long size, int column) {
			return realName + "_[R" + toHex(blockSize) + "," + toHex(size) + "," + toHex(column) + "," + toHex(dataSize) + "," + toHex(checksumSize) + ")";
		}
		
	}
	
	private static void autostripe(MeldFs meldfs, AutoStriperFactory striperFactory, Path vpath, int redundancy, boolean verbose) throws IOException, InterruptedException {
		if (verbose)
			System.out.println(vpath);
		try {
			if (meldfs.isDirectory(vpath)) {
				meldfs.createStripeDirs(vpath);
				Set<String> children = meldfs.ls(vpath);
				// TODO: consider striping an entire directory's contents as one file if it contains many small files
				for (String child : children) {
					Path childPath = vpath.resolve(child);
					autostripe(meldfs, striperFactory, childPath, redundancy, verbose);
				}
			}
			else if (meldfs.isSymlink(vpath)) {
				// TODO: clone the symlink across all columns
			}
			else {
				AutoStriper autoStriper = striperFactory.getStriper();
				autoStriper.stripe(vpath);
			}
		}
		catch (FilesystemException e) {
			System.err.println(e.getMessage() + " on " + vpath);
		}
	}
	
	public static boolean areEqual(byte[] b1, byte[] b2) {
		if (b1.length != b2.length)
			return false;
		for (int i = 0; i < b1.length; i++)
			if (b1[i] != b2[i])
				return false;
		return true;
	}

	final static ThreadLocal<MessageDigest> _digest = new ThreadLocal<MessageDigest>() {
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("SHA1");
			}
			catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
				return null;
			}
		};
	};
	final static ThreadLocal<ByteBuffer> _buffer = new ThreadLocal<ByteBuffer>() {
		protected ByteBuffer initialValue() {
			return ByteBuffer.allocateDirect(4096);
		};
	};
	
	public static byte[] sha1(ScatteringByteChannel channel) throws IOException {
		MessageDigest md = _digest.get();
		if (md == null)
			return new byte[0];
		md.reset();
		ByteBuffer bb = _buffer.get();
		bb.clear();
		while (-1 != channel.read(bb)) {
			bb.flip();
			md.update(bb);
			bb.compact();
		}
		return md.digest();
	}

	private static Path sanitize(MeldFs meldfs, Path vpath) {
		if (vpath == meldfs.rootPath)
			return vpath;
		if (vpath.equals(vpath.getRoot()))
			return meldfs.rootPath;
		if (vpath.isAbsolute())
			 vpath = vpath.subpath(0, vpath.getNameCount());
		return vpath;
	}

	
}
