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
	unsigned char **ppColumns = (unsigned char**)malloc(sizeof(unsigned char*) * columnCount);
	unsigned char **ppMappedColumns;
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
		if (0 != ((uint64_t)ppColumns[i] % 16))
			unaligned = 1;
	}
	ppMappedColumns = getMappedColumns(columnCount, ppColumns, env, recoveryMap);
	// get the matrix address
	unsigned char *pMatrix = (*env)->GetDirectBufferAddress(env, matrix);
	// initialize the galois field lookup table addresses
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

	int jagged = 0;
	for (int i = 0; i < dataSize; i++)
		if (pColumnSizes[i] < height)
			jagged = 1;

	int result = 0;

	unsigned char symbol;

	__m128i symbolgroup;
	unsigned char* codegroup;
	__m128i zero = _mm_setzero_si128();
	__m128i cmp;
	for (int c = 0; c < columnCount; c++) {
		if (calcMask & (1 << c)) {
			unsigned char *pColumn = ppColumns[c];
			if (jagged || (height % SSEBYTES) != 0 || unaligned) {
				for (int position = 0; position < height; position++) {
					symbol = 0;
					// the checksum is equal to the gf sum of the gf products of each data symbol by the corresponding value in the coding matrix
					for (int k = 0; k < dataSize; k++) {
						unsigned char datum = (position < pColumnSizes[k]) ? ppColumns[k][position] : 0;
						if (datum == 0)
							continue;
						unsigned char code = pMatrix[c * dataSize + k];
						if (code == 0)
							continue;
						symbol ^= pGfinvlog[pGflog[code] + pGflog[datum]];
					}
					pColumn[position] = symbol;
				}
			}
			else {
				// SSE2
				codegroup = pMatrix + c * dataSize;
				for (int position = 0; position < height; position += SSEBYTES) {
					symbolgroup = _mm_setzero_si128();
					for (int k = 0; k < dataSize; k++) {
						unsigned char logb = pGflog[codegroup[k]];
						unsigned char* datagroupBytes = ppMappedColumns[k] + position;
						unsigned char calcgroupBytes[SSEBYTES];
						for (int i = 0; i < SSEBYTES; i++)
							calcgroupBytes[i] = pGfinvlog[pGflog[datagroupBytes[i]]+logb];
						__m128i datagroup = *(__m128i*)datagroupBytes;
						cmp = _mm_cmpeq_epi8(datagroup, zero);
						__m128i calcgroup = *(__m128i*)calcgroupBytes;
						calcgroup = _mm_andnot_si128(cmp, calcgroup);
						symbolgroup = _mm_xor_si128(symbolgroup, calcgroup);
					}
					*(__m128i*)(pColumn + position) = symbolgroup;
				}
			}
			result += height;
		}
	}
	//------------------------------------------------------------

	if (ppMappedColumns != ppColumns) {
		free(ppMappedColumns);
	}
	if (isUsingNonDirectBuffers) {
		for (int i = 0; i < columnCount; i++) {
			jobject column = (*env)->GetObjectArrayElement(env, columns, i);
			void* addr = (*env)->GetDirectBufferAddress(env, column);
			if (addr == 0) {
				jobject bytes = (*env)->CallObjectMethod(env, column, byteBufferArrayMethod);
				// TODO: pass JNI_ABORT as the last parameter unless we wrote to this column
				(*env)->ReleaseByteArrayElements(env, bytes, (jbyte *)ppColumns[i], ((1 << i) & calcMask) ? 0 : JNI_ABORT);
			}
		}
	}
	(*env)->ReleaseIntArrayElements(env, lengths, pColumnSizes, 0);
	free(ppColumns);

	return result;
	//*/
}
