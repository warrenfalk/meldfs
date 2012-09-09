package warrenfalk.reedsolomon;

import java.nio.ByteBuffer;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

/**
 * Example usage of the Reed-Solomon library
 * @author Warren Falk
 */
public class Examples {

	public static void main(String[] args) {
		// Construct a coding domain that uses 8 symbols with 5 data symbols and 3 checksum symbols that can survive the loss of any 3
		ReedSolomonCodingDomain domain = new ReedSolomonCodingDomain(5, 3);
		// construct test buffers (of just one byte)
		// fill in the data bytes (note that 8 bits is all that is allowed per symbol in the default coding domain's GaloisField, GF256)
		ByteBuffer[] words = new ByteBuffer[] {
				ByteBuffer.wrap(new byte[] { 0x03 }),
				ByteBuffer.wrap(new byte[] { 0x14 }),
				ByteBuffer.wrap(new byte[] { 0x15 }),
				ByteBuffer.wrap(new byte[] { 0x00 }),
				ByteBuffer.wrap(new byte[] { 0x65 }),

				ByteBuffer.wrap(new byte[] { 0x00 }),
				ByteBuffer.wrap(new byte[] { 0x00 }),
				ByteBuffer.wrap(new byte[] { 0x00 }),
		};
		System.out.println("Buffers with data symbols:");
		dumpBuffers(words);
		// get a coder that can calculate the checksums
		Coder coder = domain.getChecksumCoder();
		// calculate the checksum symbols
		coder.calculate(words);
		System.out.println("Buffers with checksum symbols added:");
		dumpBuffers(words);
		// now erase two words and one checksum
		words[2].put(0, (byte)0);
		words[4].put(0, (byte)0);
		words[7].put(0, (byte)0);
		System.out.println("Buffers with simulated erasures:");
		dumpBuffers(words);
		// create a bitmask indicating which are valid and construct a recovery coder from that
		int valid = ~((1 << 2) | (1 << 4) | (1 << 7));
		Coder recoverer = domain.createCoder(valid);
		recoverer.calculate(words);
		// and they are recovered.
		System.out.println("Reconstructed code word:");
		dumpBuffers(words);
	}
	
	private static void dumpBuffers(ByteBuffer[] words) {
		System.out.print("[ ");
		for (int i = 0; i < words.length; i++)
			words[i].clear();
		boolean hasRemaining = true;
		for (;hasRemaining;) {
			hasRemaining = false;
			for (int i = 0; i < words.length; i++) {
				System.out.print(" " + hex(words[i].get()) + " ");
				if (words[i].hasRemaining())
					hasRemaining = true;
			}
		}
		System.out.println(" ]");
	}

	final static char[] hex = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	static String hex(byte b) {
		return "" + hex[(b & 0xf0) >> 4] + hex[b & 0xf];
	}
	
	
}
