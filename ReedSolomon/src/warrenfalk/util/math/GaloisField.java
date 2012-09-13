package warrenfalk.util.math;

public final class GaloisField {
	public final int bits;
	public final int primitive;
	public final int size;
	public final int mask;
	public final int[] log;
	public final int[] invlog;
	public final int[] inv;
	
	public final static GaloisField GF4 = new GaloisField(2, 7);
	public final static GaloisField GF8 = new GaloisField(3, 11); // or 13
	public final static GaloisField GF16 = new GaloisField(4, 19); // or 25
	public final static GaloisField GF32 = new GaloisField(5, 37); // or 41 47 55 59 61
	public final static GaloisField GF64 = new GaloisField(6, 67); // or 91 97 103 109 115
	public final static GaloisField GF128 = new GaloisField(7, 131); // or 137 143 145 157 167 171 185 191 193 203 211 213 229 239 241 247 253
	public final static GaloisField GF256 = new GaloisField(8, 285); // or 299 301 333 351 355 357 361 369 391 397 425 451 463 487 501
	public final static GaloisField[] GF = new GaloisField[] { null, null, GF4, GF8, GF16, GF32, GF64, GF128, GF256 };
	
	public GaloisField(final int bits, final int primitive) {
		this.bits = bits;
		this.primitive = primitive;
		assert isValidPrimitive(bits, primitive);
		this.size = 1 << bits;
		this.mask = this.size - 1;
		log = new int[size];
		invlog = new int[size << 1];
		inv = new int[size];
		int b = 1;
		for (int i = 0; i < size; i++) {
			log[b] = i;
			invlog[i] = b;
			b = b << 1;
			if (0 != (b & size))
				b = b ^ primitive;
		}
		for (int i = size - 1; i < (size * 2); i++) {
			invlog[i] = invlog[i - (size - 1)];
		}
		// calculate inverses
		for (int i = 2; i < size; i++) {
			for (int j = 2; j < size; j++) {
				int p = mult(i, j);
				if (p == 1) {
					inv[i] = j;
					break;
				}
			}
		}
	}
	
	@Override
	public String toString() {
		return "GF" + size;
	}
	
	public int add(int a, int b) {
		return a ^ b;
	}

	public int inv(int v) {
		if (v == 0)
			throw new ArithmeticException("divide by zero");
		if (v == 1)
			return 1;
		return inv[v];
	}
	
	public int div(int dividend, int divisor) {
		return mult(dividend, inv(divisor));
	}
	
	public int mult(int a, int b) {
		if (b == 0 || a == 0)
			return 0;
		if (b == 1)
			return a;
		if (a == 1)
			return b;
		return invlog[log[a] + log[b]];
		/*
		// This performs multiplication without log tables
		int t = 0;
		for (; a != 0; a >>= 1) {
			if (0 != (a & 1))
				t ^= b;
			b <<= 1;
		}
		int bit = 1 << (bits + bits - 2);
		int end = 1 << (bits - 1);
		int p = primitive << (bits - 2);
		for (; bit != end; bit >>= 1) {
			if (0 != (t & bit))
				t ^= p;
			p >>= 1;
		}
		return t;
		*/
	}
	
	public static boolean isValidPrimitive(int bits, int primitive) {
		int elementCount = 1 << bits;
		if ((primitive & elementCount) == 0)
			return false;
		int[] counts = new int[elementCount];
		counts[0]++;
		counts[1]++;
		counts[2]++;
		int v = 2;
		int alpha = 2;
		for (int i = 3; i < elementCount; i++) {
			v *= alpha;
			if (v >= elementCount)
				v = (v ^ primitive) % elementCount;
			counts[v]++;
			if (counts[v] > 1)
				return false;
		}
		return true;
	}
	
	public String toBinary(int num) {
		return toBinary(bits, num);
	}

	public String toBinary(int digits, int num) {
		char[] c = new char[digits];
		while (digits > 0) {
			int digit = num % 2;
			c[--digits] = digit == 0 ? '0' : '1';
			num /= 2;
		}
		return new String(c);
	}

}
