package warrenfalk.reedsolomon;

import java.nio.ByteBuffer;
import java.util.Random;

import warrenfalk.reedsolomon.ReedSolomonCodingDomain.Coder;

public class Debug {

	public static void main(String[] args) {
		int data = 5;
		
		ByteBuffer[] columns = new ByteBuffer[] {
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
				ByteBuffer.allocateDirect(512),
		};
		
		Random rand = new Random();
		for (int i = 0; i < data; i++) {
			byte[] buffer = new byte[columns[i].limit()];
			rand.nextBytes(buffer);
			columns[i].put(buffer);
			columns[i].flip();
		}
		
		columns[0].put(0, (byte)0x71);
		columns[1].put(0, (byte)0xce);
		columns[2].put(0, (byte)0xdc);
		columns[3].put(0, (byte)0x9b);
		columns[4].put(0, (byte)0x0c);

		for (int c = 0; c < data; c++) {
			System.out.print("d" + c + ". [ ");
			for (int i = 0; i < columns[c].limit(); i++)
				System.out.print(((i % 16 == 0) ? "| " : "") + hex(columns[c].get(i)) + " ");
			System.out.println("]");
		}
		System.out.print("    --");
		for (int i = 0; i < columns[0].limit(); i++)
			System.out.print(((i % 16 == 0) ? "+-" : "") + "---");
		System.out.println("-");
		
		// create a canonical java-implemented coder
		ReedSolomonCodingDomain jdomain = new ReedSolomonCodingDomain(data, columns.length - data);
		Coder jcoder = jdomain.getChecksumCoder();
		jcoder.nativeHelper = null;
		
		// create the native coder
		ReedSolomonCodingDomain domain = new ReedSolomonCodingDomain(data, columns.length - data);
		Coder coder = domain.getChecksumCoder();
		
		int checksumColumnToCheck = 0;
		
		// get the canonical checksum for the second checksum column
		jcoder.calculate(columns, (1 + checksumColumnToCheck) << data);
		byte[] expected = new byte[columns[data + checksumColumnToCheck].limit()];
		columns[data + checksumColumnToCheck].get(expected);
		for (int i = 0; i < columns[data + checksumColumnToCheck].limit(); i++)
			columns[data + checksumColumnToCheck].put(i, (byte)0);
		
		// do the native coding
		coder.calculate(columns, (1 + checksumColumnToCheck) << data);
		
		// compare
		boolean fail = false;
		for (int i = 0; i < columns[data + checksumColumnToCheck].limit(); i++) {
			byte e = expected[i];
			byte a = columns[data + checksumColumnToCheck].get(i);
			if (a != e)
				fail = true;
		}
		if (fail) {
			System.out.println("FAIL");
			System.out.print("exp [ ");
			for (int i = 0; i < columns[0].limit(); i++)
				System.out.print(((i % 16 == 0) ? "| " : "") + hex(expected[i]) + " ");
			System.out.println("]");
			System.out.print("act [ ");
			for (int i = 0; i < columns[0].limit(); i++)
				System.out.print(((i % 16 == 0) ? "| " : "") + hex(columns[data + checksumColumnToCheck].get(i)) + " ");
			System.out.println("]");
			System.out.print("      ");
			for (int i = 0; i < columns[0].limit(); i++)
				System.out.print(((i % 16 == 0) ? "| " : "") + ((columns[data + checksumColumnToCheck].get(i) == expected[i]) ? "   " : "^^ " ));
			System.out.println();
			System.exit(1);
		}
		else {
			System.out.println("PASS");
		}

		// now do a timed sprint of coding
		long start = System.nanoTime();
		for (int i = 0; i < 200000; i++) {
			coder.calculate(columns, 1 << data);
			coder.calculate(columns, 2 << data);
		}
		long end = System.nanoTime();
		System.out.println((double)(end - start) / 1000000000.0);
	}

	final static char[] hex = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
	static String hex(byte b) {
		return "" + hex[(b & 0xf0) >> 4] + hex[b & 0xf];
	}
	
}
