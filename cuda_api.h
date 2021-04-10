#ifndef CUDA_API_C
#define CUDA_API_C

#include "serial_sort.h"

extern void CU_Init();
extern elem* CU_cudaAlloc(size_t byte);
extern void CU_cudaFree(elem* ptr);
extern void CU_OddEvenNetworkSort(elem* begin, elem* end);
#endif
