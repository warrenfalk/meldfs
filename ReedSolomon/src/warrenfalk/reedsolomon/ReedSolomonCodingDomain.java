package warrenfalk.reedsolomon;

import warrenfalk.util.math.GaloisField;
import warrenfalk.util.math.MatrixR;

/**
 * <p>A Reed-Solomon coder</p>
 * @author Warren Falk
 */
public class ReedSolomonCodingDomain {
	private final MatrixR codingMatrix;
	private final int dataSize;
	private final int checksumSize;
	private final int dataMask;
	private final int checksumMask;
	private final GaloisField gf;
	private final Coder checksumCoder;
	
	/**
	 * Prepares a new Reed-Solomon coding domain that fits the specified parameters.
	 * 
	 * <p>The resulting code words will be of <code>dataSize + checksumSize</code> symbols long,
	 * and can be reconstructed after the loss of up to <code>checksumSize</code> of any symbols.</p>
	 * 
	 * <p>Each symbol can be as many bits wide as the Galois Field specified.
	 * The Galois Field range then must be below dataSize + checksumSize.
	 * (e.g. a GF16 (aka GF(2^4)) can support any case where dataSize + checksumSize < 16)
	 * </p>
	 * 
	 * @param dataSize the number of symbols in each code word that carry the data
	 * @param checksumSize the number of symbols in each code word that carry the checksum data
	 * @param gf the Galois field to use
	 */
	public ReedSolomonCodingDomain(int dataSize, int checksumSize, GaloisField gf) {
		if (gf.size <= dataSize + checksumSize)
			throw new IllegalArgumentException("Specified an inadequate GaloisField for code word size of " + (dataSize + checksumSize));
		this.gf = gf;
		this.dataSize = dataSize;
		this.checksumSize = checksumSize;
		this.dataMask = (1 << dataSize) - 1;
		this.checksumMask = ((1 << checksumSize) - 1) << dataSize;

		// Create a coding matrix with n + m rows and n columns.
		// adopting system described in http://web.eecs.utk.edu/~plank/plank/papers/CS-96-332.pdf
		codingMatrix = new MatrixR(dataSize + checksumSize, dataSize);
		// the coding matrix is a vandermonde matrix which has its top n x n submatrix converted to an identity through elementary column operations
		for (int i = 0; i < (checksumSize + dataSize); i++) {
			codingMatrix.put(i, 0, 1);
			codingMatrix.put(i, 1, i);
			for (int j = 2; j < dataSize; j++)
				codingMatrix.put(i, j, gf.mult(i, codingMatrix.get(i, j - 1)));
		}
		// convert top to identity (start at 1 because row 0 is already identity)
		for (int i = 1; i < dataSize; i++) {
			int p = codingMatrix.get(i, i);
			if (p == 0) {
				for (int k = i + 1; k < (dataSize + checksumSize); k++) {
					p = codingMatrix.get(k, i);
					if (p != 0) {
						codingMatrix.swapRows(i, k);
						break;
					}
				}
			}
			if (p != 1) {
				p = gf.inv(p);
				codingMatrix.multColumn(gf, i, p);
			}
			for (int j = 0; j < dataSize; j++) {
				if (j == i)
					continue;
				int v = codingMatrix.get(i, j);
				if (v != 0)
					codingMatrix.subColumn(gf, j, i, v);
			}
		}

		// create the single checksum coder
		checksumCoder = new Coder(dataMask);
	}
	
	public ReedSolomonCodingDomain(int dataSize, int checksumSize) {
		this(dataSize, checksumSize, GaloisField.GF256);
	}
	
	interface SymbolCoder {
		void calc(int[] symbols);
	}
	
	/**
	 * Calculates checksums or missing symbols from Reed-Solomon code words
	 * @author Warren Falk
	 *
	 */
	public class Coder {
		final long validMask;
		final MatrixR recoveryMatrix;
		final int[] validSymbolMap;
		
		/**
		 * Construct a coder which can calculate symbols from the symbols specified in <code>validMask</code> 
		 * @param validMask a bitmask specifying which symbols passed to the <code>calculate()</code> function are valid
		 */
		public Coder(long validMask) {
			this.validMask = validMask & (dataMask | checksumMask);
			
			// if all the data symbols are valid, then this is just a checksum coder (i.e. skip initialization of the data recovery structures)
			if (validMask != (dataMask & validMask)) {
				// this coder will need to recover data symbols
				// to do this, a recovery matrix specialized for this scenario of invalid symbols must be created from the master recovery matrix

				recoveryMatrix = new MatrixR(dataSize, dataSize);
				validSymbolMap = new int[dataSize];
				
				int codingMatrixRow = 0;
				for (int recoveryRow = 0; recoveryRow < dataSize; codingMatrixRow++) {
					// if this symbol is valid
					if (0 != (validMask & (1 << codingMatrixRow))) {
						recoveryMatrix.copyRow(recoveryRow, codingMatrix, codingMatrixRow);
						validSymbolMap[recoveryRow] = codingMatrixRow;
						recoveryRow++;
					}
				}
				recoveryMatrix.invert(gf);
			}
			else {
				recoveryMatrix = null;
				validSymbolMap = null;
			}
		}
		
		/**
		 * Calculates missing symbols in the code word from existing valid symbols in the code word. 
		 * If any invalid checksum symbols are requested, all invalid data symbols, if any, will be automatically requested.
		 * Any symbols that were specified as valid for this coder will be automatically unrequested
		 * @param word the code word with valid symbols filled in
		 * @param calcMask a bitmask specifying which symbols to calculate
		 */
		public void calculate(int[] word, long calcMask) {
			// automatically clear any bits for calc that are already valid as there's nothing to do for these.
			calcMask &= ~validMask;
			// calculation of checksum values requires all data values to be valid. So if any data values are invalid and a checksum value was requested, automatically request the data symbols also
			boolean invalidChecksum = 0 != (calcMask & checksumMask);
			if (invalidChecksum)
				calcMask |= (~validMask & dataMask);
			// calculate data fields first
			if (0 != (calcMask & dataMask)) {
				int bit = 1;
				for (int index = 0; index < dataSize; index++) {
					if (0 != (bit & calcMask)) {
						int symbol = 0;
						// TODO: it's theoretically possible here to store the logarithms in the gf.mult() for the factor coming from the recovery matrix
						for (int j = 0; j < dataSize; j++)
							symbol = gf.add(symbol, gf.mult(recoveryMatrix.get(index, j), word[validSymbolMap[j]]));
						word[index] = symbol;
					}
					bit <<= 1;
				}
			}
			// calculate checksum
			if (invalidChecksum) {
				int bit = 1 << dataSize;
				for (int index = 0; index < checksumSize; index++) {
					if (0 != (bit & calcMask)) {
						int symbol = 0;
						// the checksum is equal to the sum of the products of each data symbol by the corresponding value in the coding matrix
						// TODO: it's theoretically possible here to store the logarithms in the gf.mult() for the factor coming from the coding matrix
						for (int k = 0; k < dataSize; k++)
							symbol = gf.add(symbol, gf.mult(word[k], codingMatrix.get(dataSize + index, k)));
						word[dataSize + index] = symbol;
					}
					bit <<= 1;
				}
			}
		}
		
		/**
		 * Automatically calculates all invalid symbols
		 * @param codeWord the code word
		 */
		public void calculate(int[] codeWord) {
			calculate(codeWord, ~validMask & (dataMask | checksumMask));
		}
		
	}

	/**
	 * Recalculates the checksum symbols after a data symbol change.
	 * 
	 * This function doesn't require a specialized coder and can recalculate checksums without reading the other data symbols.
	 * 
	 * @param symbols a buffer to read and write checksums to (data symbols are not read from or written to this buffer)
	 * @param offset the offset in symbols to the 0th checksum
	 * @param dataIndex which data symbol is being modified
	 * @param oldData the previous symbol
	 * @param newData the new symbol
	 */
	public void recalcChecksum(int[] symbols, int offset, int dataIndex, int oldData, int newData) {
		// TODO: it's theoretically possible here to store the logarithms in the gf.mult() for the factor coming from the coding matrix
		int diff = gf.add(newData, oldData);
		for (int i = 0; i < checksumSize; i++)
			symbols[offset + i] = gf.add(symbols[offset + i], gf.mult(codingMatrix.get(dataSize + i, dataIndex), diff));
	}
	
	/**
	 * Get a bitmask with only bits representing data symbols set
	 * @return bitmask with only bits representing data symbols set
	 */
	public long getDataMask() {
		return dataMask;
	}
	
	/**
	 * Get a bitmask with only bits representing checksum symbols set
	 * @return bitmask with only bits representing checksum symbols set
	 */
	public long getChecksumMask() {
		return checksumMask;
	}

	/**
	 * Gets the checksum coder.
	 * <p>Equivalent to <code>createCoder(getChecksumMask())</code></p>
	 * @return the checksum coder
	 */
	public Coder getChecksumCoder() {
		return checksumCoder;
	}
	
	/**
	 * Creates a new coder capable of coding when only the specified symbols are valid
	 * @param validMask a bitmask specifying which symbols are valid
	 * @return a new coder
	 */
	public Coder createCoder(long validMask) {
		if (validMask == dataMask)
			return checksumCoder;
		return new Coder(validMask);
	}
	
}