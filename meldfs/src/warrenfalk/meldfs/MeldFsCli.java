package warrenfalk.meldfs;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;

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
	public static int ls(Iterator<String> args) {
		// TODO: implement
		return 1;
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
