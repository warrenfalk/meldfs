package warrenfalk.meldfs;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import warrenfalk.fuselaj.FilesystemException;

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
	public static int autostripe(Iterator<String> args) throws IOException {
		// TODO: implement
		MeldFsProperties props = new MeldFsProperties();
		for (Path source : props.getSources()) {
			System.out.println(source);
		}
		return 1;
	}

}
