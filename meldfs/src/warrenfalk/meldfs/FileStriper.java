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
	
	final StripeCoder coder;
	final int dataSize;
	final int checksumSize;
	final int ringBufferSize;
	int blockSize;
	
	public FileStriper(StripeCoder coder, int dataSize, int checksumSize, int ringBufferSize) {
		this.coder = coder;
		this.dataSize = dataSize;
		this.checksumSize = checksumSize;
		this.ringBufferSize = ringBufferSize;
	}
	
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
		
		FileStriper striper = new FileStriper(stripeCoder, dataSize, checksumSize, ringBufferSize);
		
		// Open the input file and output files
		FileSystem fs = FileSystems.getDefault();
		Path inputPath = fs.getPath(inputArg);
		Path[] outputPaths = new Path[outputArgs.size()];
		for (int i = 0; i < outputArgs.size(); i++)
			outputPaths[i] = fs.getPath(outputArgs.get(i));
		
		long start = System.nanoTime();
		ChannelStriper channelStriper = striper.createChannelStriper(blockSize);
		striper.stripe(channelStriper, inputPath, outputPaths);
		long end = System.nanoTime();
		if (showPerformanceIndicators) {
			System.out.println("Wall Clock: " + format(end - start));
			System.out.println("      Read: " + format(channelStriper.readTime));
			System.out.println("     Write: " + format(channelStriper.totalWriteTime));
			for (int c = 0; c < outputPaths.length; c++)
				System.out.println("  Write[" + c + "]: " + format(channelStriper.writeTime[c]));
			System.out.println("      Calc: " + format(channelStriper.calcTime.longValue()));
		}
	}
	
	public ChannelStriper createChannelStriper(int blockSize) {
		return new ChannelStriper(coder, blockSize, dataSize, checksumSize, ringBufferSize);		
	}
	
	public void stripe(Path inputPath, Path[] outputPaths, int blockSize) throws IOException, InterruptedException {
		ChannelStriper channelStriper = createChannelStriper(blockSize);
		stripe(channelStriper, inputPath, outputPaths);
	}
	
	public void stripe(ChannelStriper striper, Path inputPath, Path[] outputPaths) throws IOException, InterruptedException {
		FileChannel inputChannel = null;
		FileChannel[] outputChannels = new FileChannel[outputPaths.length];
		try {
			inputChannel = FileChannel.open(inputPath, StandardOpenOption.READ);
			for (int i = 0; i < outputChannels.length; i++)
				outputChannels[i] = FileChannel.open(outputPaths[i], StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			striper.stripe(inputChannel, outputChannels);
		}
		finally {
			if (inputChannel != null)
				inputChannel.close();
			for (int i = 0; i < outputChannels.length; i++)
				if (outputChannels[i] != null)
					outputChannels[i].close();
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
