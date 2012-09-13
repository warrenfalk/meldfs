package warrenfalk.util.math;

import java.util.Formatter;

/**
 * Represents a matrix which is optimized for columnar access (i.e. consecutive accesses to elements in the same column)
 * @author Warren Falk
 *
 */
public final class MatrixR {
	public final int[] values;
	public final int width;
	public final int height;
	
	public MatrixR(final int rows, final int cols) {
		height = rows;
		width = cols;
		values = new int[rows * cols];
	}
	
	public MatrixR(final int[][] source) {
		height = source.length;
		width = source[0].length;
		values = new int [height * width];
		for (int i = 0; i < height; i++)
			for (int j = 0; j < width; j++)
				put(i, j, source[i][j]);
	}
	
	public int get(final int row, final int col) {
		return values[row * width + col];
	}
	
	public void put(final int row, final int col, final int value) {
		values[row * width + col] = value;
	}
	
	public void swapRows(final int rowA, final int rowB) {
		int t;
		for (int i = 0; i < width; i++) {
			t = get(rowA, i);
			put(rowA, i, get(rowB, i));
			put(rowB, i, t);
		}
	}
	
	public void multRow(final GaloisField gf, final int row, final int factor) {
		for (int j = 0; j < width; j++)
			put(row, j, gf.mult(factor, get(row, j)));
	}
	
	public void subRow(final GaloisField gf, final int destRow, final int sourceRow, final int coefficient) {
		for (int j = 0; j < width; j++)
			put(destRow, j, gf.add(get(destRow, j), gf.mult(coefficient, get(sourceRow, j))));
	}
	
	public void copyRow(int destRow, MatrixR source, int sourceRow) {
		System.arraycopy(source.values, sourceRow * width, values, destRow * width, width);
	}
	
	public void swapColumns(final int colA, final int colB) {
		int t;
		int height = values.length / width;
		for (int i = 0; i < height; i++) {
			t = get(i, colA);
			put(i, colA, get(i, colB));
			put(i, colB, t);
		}
	}
	
	public void multColumn(final GaloisField gf, final int col, final int factor) {
		int height = values.length / width;
		for (int i = 0; i < height; i++)
			put(i, col, gf.mult(factor, get(i, col)));
	}
	
	public void subColumn(final GaloisField gf, final int col, final int factorCol, final int coefficient) {
		int height = values.length / width;
		for (int i = 0; i < height; i++)
			put(i, col, gf.add(get(i, col), gf.mult(coefficient, get(i, factorCol))));
	}
	


	@Override
	public String toString() {
		int height = values.length / width;
		StringBuilder sb = new StringBuilder();
		Formatter f = new Formatter(sb);
		int maxLen = 1;
		for (int i = 0; i < values.length; i++) {
			int len = String.valueOf(values[i]).length();
			if (len > maxLen)
				maxLen = len;
		}
		String format = "%" + maxLen + "d"; 
		for (int i = 0; i < height; i++) {
			sb.append("[");
			for (int j = 0; j < width; j++) {
				sb.append(" ");
				f.format(format, get(i, j));
			}
			sb.append(" ]\r\n");
		}
		return sb.toString();
	}
	
	public String toStringLiteral() {
		int height = values.length / width;
		StringBuilder sb = new StringBuilder();
		Formatter f = new Formatter(sb);
		int maxLen = 1;
		for (int i = 0; i < values.length; i++) {
			int len = String.valueOf(values[i]).length();
			if (len > maxLen)
				maxLen = len;
		}
		String format = "%" + maxLen + "d";
		sb.append("{\r\n");
		for (int i = 0; i < height; i++) {
			sb.append("    {");
			for (int j = 0; j < width; j++) {
				f.format(format, get(i, j));
				sb.append(",");
			}
			sb.append(" },\r\n");
		}
		sb.append("}");
		return sb.toString();
	}
	
	public void invert(GaloisField gf) {
		int height = values.length / width;
		
		// create a double-wide matrix, filling in the right half with the identity matrix
		MatrixR matrix = new MatrixR(height, width * 2);
		for (int i = 0; i < height; i++) {
			matrix.put(i, i + width, 1);
			for (int j = 0; j < width; j++)
				matrix.put(i, j, get(i, j));
		}
		
		// get the left matrix to row-echelon form
		for (int j = 0; j < width; j++) {
			int p = matrix.get(j, j);
			if (p == 0) {
				for (int k = j + 1; k < width; k++) {
					p = matrix.get(k, j);
					if (p != 0) {
						matrix.swapRows(j, k);
						break;
					}
				}
			}
			if (p != 1) {
				p = gf.inv(p);
				matrix.multRow(gf, j, p);
			}
			for (int i = j + 1; i < width; i++) {
				int v = matrix.get(i, j);
				if (v != 0)
					matrix.subRow(gf, i, j, v);
			}
		}
		
		// now use back-substitution to fix the upper right of the left matrix
		for (int j = width - 1; j > 0; j--) {
			for (int i = j - 1; i >= 0; i--) {
				int p = matrix.get(i, j);
				if (p != 0)
					matrix.subRow(gf, i, j, p);
			}
		}
		
		// the right matrix is now inverted.  Copy it onto the original array
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++)
				put(i, j, matrix.get(i, j + width));
		}
	}


}
