package warrenfalk.util.math;

import static org.junit.Assert.*;
import static warrenfalk.util.math.GaloisField.GF16;
import static warrenfalk.util.math.GaloisField.GF256;

import java.util.Arrays;

import org.junit.Test;

import warrenfalk.util.math.GaloisField;

public class TestGaloisField {

	@Test
	public void testAdd() {
		for (int y = 0; y < 16; y++) {
			for (int x = 0; x < 16; x++) {
				assertEquals(GF16.add(x, y), x ^ y);
				assertEquals(GF256.add(x, y), x ^ y);
			}
		}
	}
	
	@Test
	public void testInv() {
		for (int bits = 2; bits <= 8; bits++) {
			GaloisField gf = GaloisField.GF[bits];
			int elements = (int)Math.pow(2, bits);
			for (int x = 1; x < elements; x++)
				assertEquals("Testing that 1/" + x + " * " + x + " in GF" + elements + " == 1", 1, gf.mult(x, gf.inv(x)));
		}
	}
	
	@Test
	public void testMult_known() {
		// Test known
		assertEquals(GF16.mult(0, 0), 0);
		assertEquals(GF16.mult(1, 0), 0);
		assertEquals(GF16.mult(2, 0), 0);
		assertEquals(GF16.mult(3, 0), 0);
		assertEquals(GF16.mult(4, 0), 0);
		assertEquals(GF16.mult(5, 0), 0);
		assertEquals(GF16.mult(6, 0), 0);
		assertEquals(GF16.mult(7, 0), 0);
		assertEquals(GF16.mult(8, 0), 0);
		assertEquals(GF16.mult(9, 0), 0);
		assertEquals(GF16.mult(10, 0), 0);
		assertEquals(GF16.mult(11, 0), 0);
		assertEquals(GF16.mult(12, 0), 0);
		assertEquals(GF16.mult(13, 0), 0);
		assertEquals(GF16.mult(14, 0), 0);
		assertEquals(GF16.mult(15, 0), 0);
		assertEquals(GF16.mult(0, 1), 0);
		assertEquals(GF16.mult(1, 1), 1);
		assertEquals(GF16.mult(2, 1), 2);
		assertEquals(GF16.mult(3, 1), 3);
		assertEquals(GF16.mult(4, 1), 4);
		assertEquals(GF16.mult(5, 1), 5);
		assertEquals(GF16.mult(6, 1), 6);
		assertEquals(GF16.mult(7, 1), 7);
		assertEquals(GF16.mult(8, 1), 8);
		assertEquals(GF16.mult(9, 1), 9);
		assertEquals(GF16.mult(10, 1), 10);
		assertEquals(GF16.mult(11, 1), 11);
		assertEquals(GF16.mult(12, 1), 12);
		assertEquals(GF16.mult(13, 1), 13);
		assertEquals(GF16.mult(14, 1), 14);
		assertEquals(GF16.mult(15, 1), 15);
		assertEquals(GF16.mult(0, 2), 0);
		assertEquals(GF16.mult(1, 2), 2);
		assertEquals(GF16.mult(2, 2), 4);
		assertEquals(GF16.mult(3, 2), 6);
		assertEquals(GF16.mult(4, 2), 8);
		assertEquals(GF16.mult(5, 2), 10);
		assertEquals(GF16.mult(6, 2), 12);
		assertEquals(GF16.mult(7, 2), 14);
		assertEquals(GF16.mult(8, 2), 3);
		assertEquals(GF16.mult(9, 2), 1);
		assertEquals(GF16.mult(10, 2), 7);
		assertEquals(GF16.mult(11, 2), 5);
		assertEquals(GF16.mult(12, 2), 11);
		assertEquals(GF16.mult(13, 2), 9);
		assertEquals(GF16.mult(14, 2), 15);
		assertEquals(GF16.mult(15, 2), 13);
		assertEquals(GF16.mult(0, 3), 0);
		assertEquals(GF16.mult(1, 3), 3);
		assertEquals(GF16.mult(2, 3), 6);
		assertEquals(GF16.mult(3, 3), 5);
		assertEquals(GF16.mult(4, 3), 12);
		assertEquals(GF16.mult(5, 3), 15);
		assertEquals(GF16.mult(6, 3), 10);
		assertEquals(GF16.mult(7, 3), 9);
		assertEquals(GF16.mult(8, 3), 11);
		assertEquals(GF16.mult(9, 3), 8);
		assertEquals(GF16.mult(10, 3), 13);
		assertEquals(GF16.mult(11, 3), 14);
		assertEquals(GF16.mult(12, 3), 7);
		assertEquals(GF16.mult(13, 3), 4);
		assertEquals(GF16.mult(14, 3), 1);
		assertEquals(GF16.mult(15, 3), 2);
		assertEquals(GF16.mult(0, 4), 0);
		assertEquals(GF16.mult(1, 4), 4);
		assertEquals(GF16.mult(2, 4), 8);
		assertEquals(GF16.mult(3, 4), 12);
		assertEquals(GF16.mult(4, 4), 3);
		assertEquals(GF16.mult(5, 4), 7);
		assertEquals(GF16.mult(6, 4), 11);
		assertEquals(GF16.mult(7, 4), 15);
		assertEquals(GF16.mult(8, 4), 6);
		assertEquals(GF16.mult(9, 4), 2);
		assertEquals(GF16.mult(10, 4), 14);
		assertEquals(GF16.mult(11, 4), 10);
		assertEquals(GF16.mult(12, 4), 5);
		assertEquals(GF16.mult(13, 4), 1);
		assertEquals(GF16.mult(14, 4), 13);
		assertEquals(GF16.mult(15, 4), 9);
		assertEquals(GF16.mult(0, 5), 0);
		assertEquals(GF16.mult(1, 5), 5);
		assertEquals(GF16.mult(2, 5), 10);
		assertEquals(GF16.mult(3, 5), 15);
		assertEquals(GF16.mult(4, 5), 7);
		assertEquals(GF16.mult(5, 5), 2);
		assertEquals(GF16.mult(6, 5), 13);
		assertEquals(GF16.mult(7, 5), 8);
		assertEquals(GF16.mult(8, 5), 14);
		assertEquals(GF16.mult(9, 5), 11);
		assertEquals(GF16.mult(10, 5), 4);
		assertEquals(GF16.mult(11, 5), 1);
		assertEquals(GF16.mult(12, 5), 9);
		assertEquals(GF16.mult(13, 5), 12);
		assertEquals(GF16.mult(14, 5), 3);
		assertEquals(GF16.mult(15, 5), 6);
		assertEquals(GF16.mult(0, 6), 0);
		assertEquals(GF16.mult(1, 6), 6);
		assertEquals(GF16.mult(2, 6), 12);
		assertEquals(GF16.mult(3, 6), 10);
		assertEquals(GF16.mult(4, 6), 11);
		assertEquals(GF16.mult(5, 6), 13);
		assertEquals(GF16.mult(6, 6), 7);
		assertEquals(GF16.mult(7, 6), 1);
		assertEquals(GF16.mult(8, 6), 5);
		assertEquals(GF16.mult(9, 6), 3);
		assertEquals(GF16.mult(10, 6), 9);
		assertEquals(GF16.mult(11, 6), 15);
		assertEquals(GF16.mult(12, 6), 14);
		assertEquals(GF16.mult(13, 6), 8);
		assertEquals(GF16.mult(14, 6), 2);
		assertEquals(GF16.mult(15, 6), 4);
		assertEquals(GF16.mult(0, 7), 0);
		assertEquals(GF16.mult(1, 7), 7);
		assertEquals(GF16.mult(2, 7), 14);
		assertEquals(GF16.mult(3, 7), 9);
		assertEquals(GF16.mult(4, 7), 15);
		assertEquals(GF16.mult(5, 7), 8);
		assertEquals(GF16.mult(6, 7), 1);
		assertEquals(GF16.mult(7, 7), 6);
		assertEquals(GF16.mult(8, 7), 13);
		assertEquals(GF16.mult(9, 7), 10);
		assertEquals(GF16.mult(10, 7), 3);
		assertEquals(GF16.mult(11, 7), 4);
		assertEquals(GF16.mult(12, 7), 2);
		assertEquals(GF16.mult(13, 7), 5);
		assertEquals(GF16.mult(14, 7), 12);
		assertEquals(GF16.mult(15, 7), 11);
		assertEquals(GF16.mult(0, 8), 0);
		assertEquals(GF16.mult(1, 8), 8);
		assertEquals(GF16.mult(2, 8), 3);
		assertEquals(GF16.mult(3, 8), 11);
		assertEquals(GF16.mult(4, 8), 6);
		assertEquals(GF16.mult(5, 8), 14);
		assertEquals(GF16.mult(6, 8), 5);
		assertEquals(GF16.mult(7, 8), 13);
		assertEquals(GF16.mult(8, 8), 12);
		assertEquals(GF16.mult(9, 8), 4);
		assertEquals(GF16.mult(10, 8), 15);
		assertEquals(GF16.mult(11, 8), 7);
		assertEquals(GF16.mult(12, 8), 10);
		assertEquals(GF16.mult(13, 8), 2);
		assertEquals(GF16.mult(14, 8), 9);
		assertEquals(GF16.mult(15, 8), 1);
		assertEquals(GF16.mult(0, 9), 0);
		assertEquals(GF16.mult(1, 9), 9);
		assertEquals(GF16.mult(2, 9), 1);
		assertEquals(GF16.mult(3, 9), 8);
		assertEquals(GF16.mult(4, 9), 2);
		assertEquals(GF16.mult(5, 9), 11);
		assertEquals(GF16.mult(6, 9), 3);
		assertEquals(GF16.mult(7, 9), 10);
		assertEquals(GF16.mult(8, 9), 4);
		assertEquals(GF16.mult(9, 9), 13);
		assertEquals(GF16.mult(10, 9), 5);
		assertEquals(GF16.mult(11, 9), 12);
		assertEquals(GF16.mult(12, 9), 6);
		assertEquals(GF16.mult(13, 9), 15);
		assertEquals(GF16.mult(14, 9), 7);
		assertEquals(GF16.mult(15, 9), 14);
		assertEquals(GF16.mult(0, 10), 0);
		assertEquals(GF16.mult(1, 10), 10);
		assertEquals(GF16.mult(2, 10), 7);
		assertEquals(GF16.mult(3, 10), 13);
		assertEquals(GF16.mult(4, 10), 14);
		assertEquals(GF16.mult(5, 10), 4);
		assertEquals(GF16.mult(6, 10), 9);
		assertEquals(GF16.mult(7, 10), 3);
		assertEquals(GF16.mult(8, 10), 15);
		assertEquals(GF16.mult(9, 10), 5);
		assertEquals(GF16.mult(10, 10), 8);
		assertEquals(GF16.mult(11, 10), 2);
		assertEquals(GF16.mult(12, 10), 1);
		assertEquals(GF16.mult(13, 10), 11);
		assertEquals(GF16.mult(14, 10), 6);
		assertEquals(GF16.mult(15, 10), 12);
		assertEquals(GF16.mult(0, 11), 0);
		assertEquals(GF16.mult(1, 11), 11);
		assertEquals(GF16.mult(2, 11), 5);
		assertEquals(GF16.mult(3, 11), 14);
		assertEquals(GF16.mult(4, 11), 10);
		assertEquals(GF16.mult(5, 11), 1);
		assertEquals(GF16.mult(6, 11), 15);
		assertEquals(GF16.mult(7, 11), 4);
		assertEquals(GF16.mult(8, 11), 7);
		assertEquals(GF16.mult(9, 11), 12);
		assertEquals(GF16.mult(10, 11), 2);
		assertEquals(GF16.mult(11, 11), 9);
		assertEquals(GF16.mult(12, 11), 13);
		assertEquals(GF16.mult(13, 11), 6);
		assertEquals(GF16.mult(14, 11), 8);
		assertEquals(GF16.mult(15, 11), 3);
		assertEquals(GF16.mult(0, 12), 0);
		assertEquals(GF16.mult(1, 12), 12);
		assertEquals(GF16.mult(2, 12), 11);
		assertEquals(GF16.mult(3, 12), 7);
		assertEquals(GF16.mult(4, 12), 5);
		assertEquals(GF16.mult(5, 12), 9);
		assertEquals(GF16.mult(6, 12), 14);
		assertEquals(GF16.mult(7, 12), 2);
		assertEquals(GF16.mult(8, 12), 10);
		assertEquals(GF16.mult(9, 12), 6);
		assertEquals(GF16.mult(10, 12), 1);
		assertEquals(GF16.mult(11, 12), 13);
		assertEquals(GF16.mult(12, 12), 15);
		assertEquals(GF16.mult(13, 12), 3);
		assertEquals(GF16.mult(14, 12), 4);
		assertEquals(GF16.mult(15, 12), 8);
		assertEquals(GF16.mult(0, 13), 0);
		assertEquals(GF16.mult(1, 13), 13);
		assertEquals(GF16.mult(2, 13), 9);
		assertEquals(GF16.mult(3, 13), 4);
		assertEquals(GF16.mult(4, 13), 1);
		assertEquals(GF16.mult(5, 13), 12);
		assertEquals(GF16.mult(6, 13), 8);
		assertEquals(GF16.mult(7, 13), 5);
		assertEquals(GF16.mult(8, 13), 2);
		assertEquals(GF16.mult(9, 13), 15);
		assertEquals(GF16.mult(10, 13), 11);
		assertEquals(GF16.mult(11, 13), 6);
		assertEquals(GF16.mult(12, 13), 3);
		assertEquals(GF16.mult(13, 13), 14);
		assertEquals(GF16.mult(14, 13), 10);
		assertEquals(GF16.mult(15, 13), 7);
		assertEquals(GF16.mult(0, 14), 0);
		assertEquals(GF16.mult(1, 14), 14);
		assertEquals(GF16.mult(2, 14), 15);
		assertEquals(GF16.mult(3, 14), 1);
		assertEquals(GF16.mult(4, 14), 13);
		assertEquals(GF16.mult(5, 14), 3);
		assertEquals(GF16.mult(6, 14), 2);
		assertEquals(GF16.mult(7, 14), 12);
		assertEquals(GF16.mult(8, 14), 9);
		assertEquals(GF16.mult(9, 14), 7);
		assertEquals(GF16.mult(10, 14), 6);
		assertEquals(GF16.mult(11, 14), 8);
		assertEquals(GF16.mult(12, 14), 4);
		assertEquals(GF16.mult(13, 14), 10);
		assertEquals(GF16.mult(14, 14), 11);
		assertEquals(GF16.mult(15, 14), 5);
		assertEquals(GF16.mult(0, 15), 0);
		assertEquals(GF16.mult(1, 15), 15);
		assertEquals(GF16.mult(2, 15), 13);
		assertEquals(GF16.mult(3, 15), 2);
		assertEquals(GF16.mult(4, 15), 9);
		assertEquals(GF16.mult(5, 15), 6);
		assertEquals(GF16.mult(6, 15), 4);
		assertEquals(GF16.mult(7, 15), 11);
		assertEquals(GF16.mult(8, 15), 1);
		assertEquals(GF16.mult(9, 15), 14);
		assertEquals(GF16.mult(10, 15), 12);
		assertEquals(GF16.mult(11, 15), 3);
		assertEquals(GF16.mult(12, 15), 8);
		assertEquals(GF16.mult(13, 15), 7);
		assertEquals(GF16.mult(14, 15), 5);
		assertEquals(GF16.mult(15, 15), 10);
	}

	@Test
	public void testMult() {
		for (int bits = 2; bits <= 8; bits++) {
			GaloisField gf = GaloisField.GF[bits];
			int elements = (int)Math.pow(2, bits);
			// first make sure that zero is the product of any two numbers of which one is zero
			for (int x = 0; x < elements; x++) {
				assertEquals(gf.mult(0, x), 0);
				assertEquals(gf.mult(x, 0), 0);
			}
			// next make sure that one is the multiplicative identity
			for (int x = 0; x < elements; x++) {
				assertEquals(gf.mult(1, x), x);
				assertEquals(gf.mult(x, 1), x);
			}
			// next ensure commutivity
			for (int y = 0; y < elements; y++) {
				for (int x = 0; x < elements; x++) {
					assertEquals(gf.mult(x, y), gf.mult(y, x));
				}
			}
			// make sure that every value is only used once in each row/column of the multiplication matrix
			int[] prod = new int[elements];
			for (int y = 1; y < elements; y++) {
				Arrays.fill(prod, 0);
				for (int x = 1; x < elements; x++) {
					int p = gf.mult(x, y);
					assertTrue(p >= 1);
					assertTrue(p < elements);
					prod[p]++;
					assertTrue(prod[p] == 1);
				}
			}
			
			// lastly check a known-working implementation
			for (int y = 0; y < elements; y++) {
				for (int x = 0; x < elements; x++) {
					int r = reference_mult(gf, x, y);
					int p = gf.mult(x, y);
					assertEquals(r, p);
				}
			}
		}
	}
	
	private int reference_mult(GaloisField gf, int a, int b) {
		// This performs multiplication without log tables
		int t = 0;
		for (; a != 0; a >>= 1) {
			if (0 != (a & 1))
				t ^= b;
			b <<= 1;
		}
		int bit = 1 << (gf.bits + gf.bits - 2);
		int end = 1 << (gf.bits - 1);
		int p = gf.primitive << (gf.bits - 2);
		for (; bit != end; bit >>= 1) {
			if (0 != (t & bit))
				t ^= p;
			p >>= 1;
		}
		return t;
	}

	@Test
	public void testDiv() {
		for (int bits = 2; bits <= 8; bits++) {
			GaloisField gf = GaloisField.GF[bits];
			int elements = (int)Math.pow(2, bits);
			for (int y = 0; y < elements; y++) {
				for (int x = 1; x < elements; x++) {
					assertEquals(y, gf.div(gf.mult(y, x), x));
				}
			}
		}		
	}
	
	@Test
	public void testIsValidPrimitive() {
		//fail("Not yet implemented");
	}

}
