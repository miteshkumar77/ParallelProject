#include <cuda.h>
#include <cuda_runtime.h>
#include "./serial_sort.h"

extern "C" {
	void CU_Init();
	elem* CU_cudaAlloc(size_t bytes);
	void CU_cudaFree(elem* ptr);
	void CU_OddEvenNetworkSort(elem* begin, elem* end);
}

cudaError_t cE;

void CU_Init(int world_rank, int world_size) {
	
	int assignedCudaDevice = -1;
	cE = cudaSuccess;
	int cudaDeviceCount = -1;

	if ((cE = cudaGetDeviceCount(&cudaDeviceCount)) != cudaSuccess) {
		fprintf(stderr, " Unable to determine cuda device count, error is %d, count is %d\n", cE, cudaDeviceCount);
		exit(EXIT_FAILURE);
	}
	
	if ((cE = cudaSetDevice(world_rank % cudaDeviceCount)) != cudaSuccess) {
		fprintf(stderr, " Unable to have rank %d set to cuda device %d, error is %d\n", world_rank, (world_rank % cudaDeviceCount), cE);
		exit(EXIT_FAILURE);
	}

	if ((cE = cudaGetDevice(&assignedCudaDevice)) != cudaSuccess) {
		fprintf(stderr, " Unable to have rank %d set to cuda device %d, error is %d\n", world_rank, (world_rank % cudaDeviceCount), cE);
		exit(EXIT_FAILURE);
	}

	if (assignedCudaDevice != (world_rank % cudaDeviceCount)) {
		fprintf(stderr, "MPI Rank %d: assignedCudaDevice: %d NOT EQUAL to (world_rank(%d) mod cudaDeviceCount(%d))\n", world_rank, assignedCudaDevice, world_rank, cudaDeviceCount);
		exit(EXIT_FAILURE);
	}
}

elem* CU_cudaAlloc(size_t bytes) {
	elem* ret = NULL;
	if ((cE = cudaMallocManaged(&ret, bytes)) != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaMallocManaged() failed with error code %d\n", cE);
		exit(EXIT_FAILURE);
	}
	return ret;
}	

void CU_cudaFree(elem* ptr) {
	if ((cE = cudaFree(ptr)) != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaFree() failed with error code %d\n", cE);
		exit(EXIT_FAILURE);
	}
}

__global__
void CU_OddEvenKernel(elem* arr, size_t n, int even, size_t numActions) {
	elem* a1 = NULL;
	elem* a2 = NULL;
	elem swp;

	size_t index = blockIdx.x * blockDim.x + threadIdx.x;
	size_t incr = blockDim.x * gridDim.x;
	if (index >= numActions) {
		return;
	}
		
	for (; index < numActions; index += incr) {
		if (even) {
			a1 = arr + index * 2;
			a2 = arr + index * 2 + 1;
		} else {
			a1 = arr + index * 2 + 1;
			a2 = arr + index * 2 + 2;
		}

		if (((size_t)(a2 - arr)) < n && (*a2) < (*a1)) {
			swp = (*a1);
			(*a1) = (*a2);
			(*a2) = swp;
		}
	}
}

void CU_OddEvenNetworkSort(elem* begin, elem* end, size_t threadsCount) {

	size_t n = end - begin + 1;
	elem* cbegin;
	cE = cudaMallocManaged(&cbegin, n * sizeof(elem));
	if (cE != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaMallocManaged() failed with error code: %d\n", cE);
		exit(EXIT_FAILURE);
	}

	cE = cudaMemcpy(cbegin, begin, n * sizeof(elem), ::cudaMemcpyHostToDevice); 
	if (cE != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaMemcpy(::HostToDevice) failed with error code: %d\n", cE);
		exit(EXIT_FAILURE);
	}

	ssize_t iters = n;
	while(iters--) {
		CU_OddEvenKernel<<<(n/2 + threadsCount)/threadsCount, threadsCount>>>(begin,
				n, (iters % 2) == 0, n/2);
		cudaDeviceSynchronize();
	}

	cE = cudaMemcpy(begin, cbegin, n * sizeof(elem), ::cudaMemcpyDeviceToHost);
	if (cE != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaMemcpy(::DeviceToHost) failed with error code: %d\n", cE);
		exit(EXIT_FAILURE);
	}

	cE = cudaFree(cbegin);
	if (cE != cudaSuccess) {
		fprintf(stderr, "ERROR: cudaFree(::DeviceToHost) failed with error code: %d\n", cE);
		exit(EXIT_FAILURE);
	}
	
}

#if 0
int main(int argc, char** argv) {

	elem* arr = CU_cudaAlloc(5 * sizeof(elem));
	arr[0] = 5;
	arr[1] = 3;
	arr[2] = 4;
	arr[3] = 7;
	arr[4] = 1;
	
	printf("NUMS:");
	for (int i = 0; i < 5; ++i) {
		printf(" %d", arr[i]);
	}
	printf("\n");

	CU_OddEvenNetworkSort(arr, arr + 4, 3);

	printf("SORTED:");
	for (int i = 0; i < 5; ++i) {
		printf(" %d", arr[i]);
	}
	printf("\n");

	CU_cudaFree(arr);
	
}
#endif






















