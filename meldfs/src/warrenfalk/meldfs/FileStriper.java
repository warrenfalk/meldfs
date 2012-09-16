package warrenfalk.meldfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain;
import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class FileStriper {
	public static void main(String[] args) throws IOException, InterruptedException {
		// No args? Print usage
		if (args.length == 0) {
			printUsage();
			System.exit(1);
		}
		
		// Set some defaults and parse arguments
		String action = null;
		String protocol = "R";
		int dataSize = -1;
		int checksumSize = -1;
		int blockSize = 512;
		int ringBufferSize = 32;
		String inputArg = null;
		ArrayList<String> outputArgs = new ArrayList<String>();
		boolean showPerformanceIndicators = false;
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
					else if (arg.equals("--show-performance"))
						showPerformanceIndicators = true;
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
		
		// Open the input file and output files
		FileSystem fs = FileSystems.getDefault();
		Path inputPath = fs.getPath(inputArg);
		Path[] outputPaths = new Path[outputArgs.size()];
		for (int i = 0; i < outputArgs.size(); i++)
			outputPaths[i] = fs.getPath(outputArgs.get(i));
		FileChannel inputChannel = FileChannel.open(inputPath, StandardOpenOption.READ);
		FileChannel[] outputChannels = new FileChannel[outputPaths.length];
		for (int i = 0; i < outputChannels.length; i++)
			outputChannels[i] = FileChannel.open(outputPaths[i], StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
		
		// Create a stripe coder, based on given protocol
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
		
		// Create our striper and begin the actual striping
		long start = System.nanoTime();
		ChannelStriper striper = new ChannelStriper(stripeCoder, blockSize, dataSize, checksumSize, ringBufferSize);
		striper.stripe(inputChannel, outputChannels);
		long end = System.nanoTime();
		if (showPerformanceIndicators) {
			System.out.println("Wall Clock: " + format(end - start));
			System.out.println("      Read: " + format(striper.readTime));
			System.out.println("     Write: " + format(striper.totalWriteTime));
			for (int c = 0; c < outputChannels.length; c++)
				System.out.println("  Write[" + c + "]: " + format(striper.writeTime[c]));
			System.out.println("      Calc: " + format(striper.calcTime.longValue()));
		}
	}
	
	/** Print the usage **/
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
		System.out.println("     --show-performance");
		System.out.println("           display performance indicators after completion");
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

	/** Format nanoseconds into seconds for printing **/
	private static String format(long nanoTime) {
		double seconds = (double)nanoTime / (double)1000000000.0;
		return "" + (Math.floor(seconds * 100.0) / 100.0);
	}
}
