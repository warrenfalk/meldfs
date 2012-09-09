package warrenfalk.reedsolomon;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
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
	
	final static char[] hex = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	static String hex(byte b) {
		return "" + hex[(b & 0xf0) >> 4] + hex[b & 0xf];
	}
	
	@Test
	public void testCalculateSingleStripe() {
		int dataSize = 4;
		int checksumSize = 2;
		int wordSize = dataSize + checksumSize;
		
		ByteBuffer[] columns = new ByteBuffer[] {
				ByteBuffer.wrap("abcdefghijklmnop".getBytes()),
				ByteBuffer.wrap("ABCDEFGHIJKLMNOP".getBytes()),
				ByteBuffer.wrap("01234567890!@#$%".getBytes()),
				ByteBuffer.wrap(":-) :-( ;-) 8^) ".getBytes()),
				ByteBuffer.wrap(new byte[16]),
				ByteBuffer.wrap(new byte[16]),
		};
		
		assert(columns.length == wordSize);
		
		ReedSolomonCodingDomain domain = new ReedSolomonCodingDomain(dataSize, checksumSize);
		Coder coder = domain.getChecksumCoder();
		
		long size = coder.calculate(columns);
		assertEquals("size result of checksum calculation", columns[0].limit() * checksumSize, size);
		
		// now simulate various failure scenarios and verify recoverability
		ByteBuffer[] recovered = new ByteBuffer[columns.length];
		// loop through all possible failure scenarios
		for (long validMask = (1 << wordSize) - 1; validMask > 0; validMask--) {
			// count how many are invalid
			int invalidCount = 0;
			for (int b = 0; b < wordSize; b++)
				if (0 == (validMask & (1 << b)))
					invalidCount++;
			// if none are invalid, there's nothing to verify
			if (invalidCount == 0)
				continue;
			// if more are invalid than we have checksums, then there is no requirement to recover from this scenario  
			if (invalidCount > checksumSize)
				continue; // not a possible recovery scenario
			
			for (int i = 0; i < wordSize; i++) {
				if (0 == (validMask & (1 << i)))
					recovered[i] = ByteBuffer.allocate(columns[0].limit());
				else
					recovered[i] = columns[i].duplicate();
			}
			
			Coder recoverer = domain.createCoder(validMask);
			size = recoverer.calculate(recovered);
			
			assertEquals("size result of recovery calculation for valid mask " + validMask, invalidCount * columns[0].limit(), size);
			
			for (int i = 0; i < wordSize; i++) {
				assertContentEqual("content of recovery buffer " + i + " for valid mask " + validMask, columns[i], recovered[i]);
			}
		}
	}

	private void assertContentEqual(String message, ByteBuffer expected, ByteBuffer actual) {
		assertTrue(message + ", comparing limits", actual.limit() >= expected.limit());
		int limit = Math.min(expected.limit(), actual.limit());
		for (int i = 0; i < limit; i++)
			assertEquals(message + ", byte at position " + i, expected.get(i), actual.get(i));
	}

}
