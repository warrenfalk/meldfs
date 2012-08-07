package warrenfalk.reedsolomon;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

/**
 * Example usage of the Reed-Solomon library
 * @author Warren Falk
 */
public class Examples {

	public static void main(String[] args) {
		// Construct a coding domain that uses 8 symbols with 5 data symbols and 3 checksum symbols that can survive the loss of any 3
		ReedSolomonCodingDomain domain = new ReedSolomonCodingDomain(5, 3);
		// construct an empty code word
		int[] word = new int[8];
		// fill in the first five bytes (note that 8 bits is all that is allowed per symbol in the default coding domain's GaloisField, GF256)
		word[0] = 0x03;
		word[1] = 0x14;
		word[2] = 0x15;
		word[3] = 0x92;
		word[4] = 0x65;
		System.out.println("Code word with data symbols:");
		dumpWord(word);
		// get a coder that can calculate the checksums
		Coder coder = domain.getChecksumCoder();
		// calculate the checksum symbols
		coder.calculate(word);
		System.out.println("Code word with checksum symbols added:");
		dumpWord(word);
		// now erase two words and one checksum
		word[2] = 0;
		word[4] = 0;
		word[7] = 0;
		System.out.println("Code word with erasures:");
		dumpWord(word);
		// create a bitmask indicating which are valid and construct a recovery coder from that
		int valid = ~((1 << 2) | (1 << 4) | (1 << 7));
		Coder recoverer = domain.createCoder(valid);
		recoverer.calculate(word);
		// and they are recovered.
		System.out.println("Reconstructed code word:");
		dumpWord(word);
	}
	
	private static void dumpWord(int[] word) {
		System.out.print("[ ");
		for (int i = 0; i < word.length; i++)
			System.out.print(" " + word[i] + " ");
		System.out.println(" ]");
	}

}
