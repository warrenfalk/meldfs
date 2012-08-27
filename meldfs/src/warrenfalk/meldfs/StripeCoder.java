package warrenfalk.meldfs;

import java.nio.ByteBuffer;

public interface StripeCoder {
	/**
	 * Calculates missing data for specified buffers from valid buffers.
	 * 
	 * <ul>
	 * <li>Each buffer is a vertical column of symbols whose rows constitute checksum-protected words</li>
	 * <li>Missing symbols are calculated as specified by <code>calcMask</code> where bit zero, if set, results in the calculation of the first buffer column.</li>
	 * <li>Positions of buffers are ignored</li>
	 * <li>Limits of the buffers to be calculated will be overwritten</li>
	 * <li>Non-calculated buffers will be used to calculate the missing symbols, but no data is "consumed", which is to say that their position is not modified</li>
	 * <li>Calculated buffers will have their limits reset and their positions set to zero making them ready for use as source buffers</li>
	 * <li>The height of the columns is determined by the limits of the buffers (position is always assumed to be zero)</li>
	 * <li>Calculated columns will have the same height as the non-calculated columns</li>
	 * <li>If columns have irregular height, all will be treated as having the same height as the tallest column by virtually padding them with zeros</li>
	 * </ul>
	 * @param columns ByteBuffers, each byte of which contributing one symbol to a word of length columns.length
	 * @param calcMask a mask specifying which columns to calculate such that bit 0, when set, causes column[0] to be calculated
	 * @return the total number of bytes injected into the calculated columns
	 */
	int calculate(ByteBuffer[] columns, int calcMask);
}
