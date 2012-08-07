package warrenfalk.reedsolomon;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;
import warrenfalk.util.math.GaloisField;

public class TestReedSolomonCoding {
	
	int dataSize = 6;
	int checksumSize = 3;
	int wordSize = dataSize + checksumSize;
	int testWords = 10240;
	ReedSolomonCodingDomain domain;
	GaloisField gf = GaloisField.GF256; // note this test currently assumes this is 8 bits or less.  Changing this will probably require a rewrite of some test code
	byte[] data = new byte[wordSize * testWords]; // 10k code words

	@Before
	public void setUp() throws Exception {
		domain = new ReedSolomonCodingDomain(dataSize, checksumSize, gf);
		Random rand = new Random();
		rand.nextBytes(data);
		// in case field is less than byte sized, shorten all bytes
		if (gf.bits < 8) {
			int mask = gf.size - 1;
			for (int i = 0; i < data.length; i++)
				data[i] &= mask;
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		int[] word = new int[wordSize];
		int[] wordIn = new int[wordSize];
		int[] wordExp = new int[wordSize];
		
		// the data starts out with all random data, including the checksum symbols, but we need to run the calculation, first, to create checksum symbols
		for (int offset = 0; offset < data.length; offset += wordSize) {
			// copy the data bytes from the buffer to the word
			for (int i = 0; i < dataSize; i++)
				word[i] = data[offset + i] & gf.mask;
			// calculate the checksum bytes
			domain.getChecksumCoder().calculate(word);
			// now put the checksum bytes into the random data
			for (int i = 0; i < checksumSize; i++) {
				int checksum = word[dataSize + i];
				// we're using an 8-bit galois field, so none of the words should exceed 8 bits.
				assertTrue("Checksum symbol fits within Galois Field length", checksum < gf.size);
				data[offset + dataSize + i] = (byte)checksum;
			}
		}
		
		// now prepare to simulate lost data and recalculate
		// we want to simulate every possible scenario, so loop through them
		for (long validMask = (1 << wordSize) - 1; validMask > 0; validMask--) {
			// If more than "checksumSize" bits in "validMask" are 0, then we can't recover in this scenario
			int invalidCount = 0;
			for (int b = 0; b < wordSize; b++) {
				if (0 == (validMask & (1 << b)))
					invalidCount++;
			}
			if (invalidCount == 0)
				continue; // nothing to check
			if (invalidCount > checksumSize)
				continue; // not a possible recovery scenario
			//System.out.println("Checking " + formatBinary(validMask) + " (" + validMask + ")");
			Coder recoverer = domain.createCoder(validMask);
			for (int offset = 0; offset < data.length; offset += wordSize) {
				// read the word from the data except for the symbols that are not valid
				for (int i = 0; i < wordSize; i++) {
					wordExp[i] = data[offset + i] & 0xFF;
					wordIn[i] = word[i] = ((validMask & (1 << i)) != 0) ? wordExp[i] : -1;
				}
				// attempt to recalculate
				recoverer.calculate(word);
				// make sure that only requested symbols were modified
				for (int i = 0; i < wordSize; i++) {
					if (0 != ((1 << i) & validMask))
						assertEquals("Coder.calculate() should not modify unrequested symbols", wordIn[i], word[i]);
					else
						assertEquals((offset / wordSize) + "th word: n:m of " + dataSize + ":" + checksumSize + " before/after check on symbol #" + i + " for code word " + formatWord(wordExp) + " with valid mask of " + formatBinary(validMask) + " (" + validMask + ")", wordExp[i], word[i]);
				}
			}
		}
		
		// now do single byte replacements, recalculating checksums
		for (int i = 0; i < dataSize; i++) {
			wordExp[i] = i;
		}
		domain.getChecksumCoder().calculate(wordExp);
		for (int offset = 0; offset < data.length; offset += wordSize) {
			for (int i = 0; i < wordSize; i++) {
				wordIn[i] = data[offset + i] & 0xFF;
			}
			for (int d = 0; d < dataSize; d++) {
				domain.recalcChecksum(wordIn, dataSize, d, wordIn[d], wordExp[d]);
			}
			for (int c = 0; c < checksumSize; c++) {
				assertEquals(wordExp[dataSize + c], wordIn[dataSize + c]);
			}
		}
	}

	private String formatBinary(long validMask) {
		StringBuilder sb = new StringBuilder();
		for (int i = wordSize - 1; i >= 0; i--)
			sb.append((0 == (validMask & (1 << i))) ? '0' : '1');
		return sb.toString();
	}

	private String formatWord(int[] word) {
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (int i = 0; i < wordSize; i++) {
			if (i != 0)
				sb.append(',');
			sb.append(word[i]);
		}
		sb.append('}');
		return sb.toString();
	}

}
