#include "warrenfalk_reedsolomon_ReedSolomonNative.h"
#include <stdlib.h>
#include <emmintrin.h>
#include <stdint.h>

#define SSEBYTES 16

jclass byteBufferClass = 0;
jmethodID byteBufferArrayMethod;
jmethodID byteBufferArrayOffsetMethod;

unsigned char ** getMappedColumns(int columnCount, unsigned char **ppColumns, JNIEnv* env, jintArray recoveryMap)
{
	if (!recoveryMap)
		return ppColumns;
	jint *map = (jint*)malloc(sizeof(jint) * columnCount);
	unsigned char **ppMapped = (unsigned char**)malloc(sizeof(unsigned char*) * columnCount);
	map = (*env)->GetIntArrayElements(env, recoveryMap, 0);
	int mapCount = (*env)->GetArrayLength(env, recoveryMap);
	for (int i = 0; i < columnCount; i++)
		 ppMapped[i] = (i < mapCount) ? ppColumns[map[i]] : ppColumns[i];
	(*env)->ReleaseIntArrayElements(env, recoveryMap, map, 0);
	return ppMapped;
}

JNIEXPORT jint JNICALL Java_warrenfalk_reedsolomon_ReedSolomonNative_nativeCalc
  (JNIEnv *env, jclass me, jint dataSize, jlong calcMask, jint height, jintArray lengths, jobjectArray columns, jobject matrix, jintArray recoveryMap, jobject gflog, jobject gfinvlog, jint gfbits, jlong gfprimitive)
{
	// get the number of columns
	jsize columnCount = (*env)->GetArrayLength(env, columns);

	// get the sizes of each column
	jint *pColumnSizes = (*env)->GetIntArrayElements(env, lengths, 0);

	// allocate an array of addresses for the columns
	// and another one to hold addresses of columns mapped for recovery
	unsigned char **ppColumns = (unsigned char**)malloc(sizeof(unsigned char*) * columnCount);
	unsigned char **ppMappedColumns;

	// if we're using direct buffers, just get the address
	// if they are indirect, then getting go through JVM calls to get to the data and remember this fact so we can release it again
	int isUsingNonDirectBuffers = 0;
	int unaligned = 0;
	for (int i = 0; i < columnCount; i++) {
		jobject column = (*env)->GetObjectArrayElement(env, columns, i);
		ppColumns[i] = (*env)->GetDirectBufferAddress(env, column);
		if (ppColumns[i] == 0) {
			if (!byteBufferClass) {
				byteBufferClass = (*env)->FindClass(env, "java/nio/ByteBuffer");
				byteBufferArrayMethod = (*env)->GetMethodID(env, byteBufferClass, "array", "()[B");
				byteBufferArrayOffsetMethod = (*env)->GetMethodID(env, byteBufferClass, "arrayOffset", "()I");
			}
			jobject bytes = (*env)->CallObjectMethod(env, column, byteBufferArrayMethod);
			ppColumns[i] = (unsigned char*)(*env)->GetByteArrayElements(env, bytes, 0);
			ppColumns[i] += (*env)->CallIntMethod(env, column, byteBufferArrayOffsetMethod);
			isUsingNonDirectBuffers = 1;
		}
		// the SSE algorithm assumes the data is aligned
		if (0 != ((uint64_t)ppColumns[i] % 16))
			unaligned = 1;
	}
	// calculate the mapped columns if a recovery map was passed in
	ppMappedColumns = getMappedColumns(columnCount, ppColumns, env, recoveryMap);

	// get the matrix address
	unsigned char *pMatrix = (*env)->GetDirectBufferAddress(env, matrix);
	// initialize the galois field lookup tables
	unsigned char *pGflog = 0;
	//int gfsize = 0;
	if (gflog) {
		pGflog = (*env)->GetDirectBufferAddress(env, gflog);
		//gfsize = (*env)->GetDirectBufferCapacity(env, gflog);
	}
	else {
		//gfsize = 1 << gfbits;
	}
	unsigned char *pGfinvlog = 0;
	if (gfinvlog)
		pGfinvlog = (*env)->GetDirectBufferAddress(env, gfinvlog);

	//------------------------------------------------------------

	// detect jagged calculations (where columns are not of equal height, we can't use the SSE algorithm for these)
	int jagged = 0;
	for (int i = 0; i < dataSize; i++)
		if (pColumnSizes[i] < height)
			jagged = 1;

	// begin with a return value of zero bytes calculated
	int result = 0;

	unsigned char symbol;

	// initialize the SSE variables
	__m128i symbolgroup;
	unsigned char* codegroup;
	__m128i zero = _mm_setzero_si128();
	__m128i cmp;

	// now do the calculation
	// the optimized SSE version can be used if it is not jagged, if height is a multiple of 16, and the buffers are all aligned
	if (jagged || (height % SSEBYTES) != 0 || unaligned) {
		for (int c = 0; c < columnCount; c++) {
			if (calcMask & (1 << c)) {
				unsigned char *pColumn = ppColumns[c];
				for (int position = 0; position < height; position++) {
					symbol = 0;
					// the checksum is equal to the gf sum of the gf products of each data symbol by the corresponding value in the coding matrix
					for (int k = 0; k < dataSize; k++) {
						// get the data byte
						unsigned char datum = (position < pColumnSizes[k]) ? ppColumns[k][position] : 0;
						if (datum == 0)
							continue; // if datum is zero, the product is zero, and xor is a noop, so just skip it
						// get the coding matrix byte
						unsigned char code = pMatrix[c * dataSize + k];
						if (code == 0)
							continue;
						// multiply them (add their logs and get the inverse log of the sum)
						symbol ^= pGfinvlog[pGflog[code] + pGflog[datum]];
					}
					// record the result
					pColumn[position] = symbol;
				}
				result += height;
			}
		}
	}
	else {
		for (int c = 0; c < columnCount; c++) {
			if (calcMask & (1 << c)) {
				unsigned char *pColumn = ppColumns[c];
				// SSE2
				// matrix row for this column
				codegroup = pMatrix + c * dataSize;
				// do 16 rows at a time with SSE
				for (int position = 0; position < height; position += SSEBYTES) {
					// initialize 16 results to zero
					symbolgroup = _mm_setzero_si128();
					// for each (mapped) data column
					for (int k = 0; k < dataSize; k++) {
						// lookup the log of the column value in the matrix
						unsigned char logb = pGflog[codegroup[k]];
						// get the data from the mapped column
						unsigned char* datagroupBytes = ppMappedColumns[k] + position;
						// have to do the multiplication 1 by 1 (no SSE optimization for lookups or GF math)
						unsigned char calcgroupBytes[SSEBYTES];
						for (int i = 0; i < SSEBYTES; i++)
							calcgroupBytes[i] = pGfinvlog[pGflog[datagroupBytes[i]]+logb];
						__m128i datagroup = *(__m128i*)datagroupBytes;
						// for the nonzero results
						cmp = _mm_cmpeq_epi8(datagroup, zero);
						__m128i calcgroup = *(__m128i*)calcgroupBytes;
						calcgroup = _mm_andnot_si128(cmp, calcgroup);
						// xor all 16 results with the running totals
						symbolgroup = _mm_xor_si128(symbolgroup, calcgroup);
					}
					// record the results
					*(__m128i*)(pColumn + position) = symbolgroup;
				}
				result += height;
			}
		}
	}
	//------------------------------------------------------------

	// free the mapped columns if it was allocated separately
	if (ppMappedColumns != ppColumns) {
		free(ppMappedColumns);
	}
	// if we were using any nondirect buffers, free all that up now
	if (isUsingNonDirectBuffers) {
		for (int i = 0; i < columnCount; i++) {
			jobject column = (*env)->GetObjectArrayElement(env, columns, i);
			void* addr = (*env)->GetDirectBufferAddress(env, column);
			if (addr == 0) {
				jobject bytes = (*env)->CallObjectMethod(env, column, byteBufferArrayMethod);
				(*env)->ReleaseByteArrayElements(env, bytes, (jbyte *)ppColumns[i], ((1 << i) & calcMask) ? 0 : JNI_ABORT);
			}
		}
	}
	// free the lengths array
	(*env)->ReleaseIntArrayElements(env, lengths, pColumnSizes, 0);
	// free the column pointers
	free(ppColumns);

	return result;
	//*/
}
