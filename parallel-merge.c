#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "./serial_sort.h"
#include "./filereader.h"

#define CLOCKS_PER_MSEC 512000

int rc, errorlen;
char errorStr[MPI_MAX_ERROR_STRING];

/* own process rank, total number of ranks */
int myrank, numranks;

/* Group consisting of all ranks */
MPI_Group world_group;

/* Store data array */
elem* dataptr;

/* Data Start Pointer */
elem* data_start;

/* Data End Pointer */
elem* data_end;

/* Path to read from */
char* frpath;

/* Path to write to */
char* fwrpath;

unsigned long long start_time;
unsigned long long end_time;
unsigned long long duration;


/*
 * 64 bit, free running clock for POWER9/AiMOS system
 *  Has 512MHz resolution.
 */
unsigned long long aimos_clock_read(void) {
  unsigned int tbl, tbu0, tbu1;

  do {
    __asm__ __volatile__("mftbu %0" : "=r"(tbu0));
    __asm__ __volatile__("mftb %0" : "=r"(tbl));
    __asm__ __volatile__("mftbu %0" : "=r"(tbu1));
  } while (tbu0 != tbu1);

  return (((unsigned long long)tbu0) << 32) | tbl;
}

/**
 * left : left MPI rank
 * right: right MPI rank
 * returns sorted array of elements in left + right
 */
elem* merge(elem* left, elem* right);

int main(int argc, char** argv) {
	/* MPI Initialization */
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &numranks);

	/* Verify that numranks is a power of 2 */
	if (bitCount(numranks) != 1) {
		fprintf(stderr, "ERROR rank(%d): number of ranks = %d wasn't a power of 2, total 1 bits: %d, should be 1\n", myrank, numranks, bitCount(numranks));
		MPI_Abort(MPI_COMM_WORLD, 0);
		return EXIT_FAILURE;
	}

	/* Input argument validation */
	if (argc != 3) {
		fprintf(stderr, "ERROR rank(%d): invalid arguments\n", myrank);
		fprintf(stderr, "USAGE: %s <frpath> <fwrpath>\n", *argv); 
		MPI_Abort(MPI_COMM_WORLD, 0);
		return EXIT_FAILURE;
	}

	/* Set fpath to read from */
	frpath = *(argv + 1);


	MPI_Offset bytes_read = readfile(myrank, numranks, &dataptr, frpath, MPI_COMM_WORLD); 
	MPI_Offset nSize = bytes_read/sizeof(elem);
	data_start = dataptr;                 // start of (buffer) array
	data_end   = dataptr + nSize - 1;     // end of (buffer) array

  /* BEGIN MERGE SORT */
	MPI_Comm parent_comm = MPI_COMM_WORLD;
	int localRank = myrank;
	//int localNumranks = numranks;
	MPI_Barrier(MPI_COMM_WORLD);   // wait for all ranks to get data into dataptr

	if (myrank == 0) {
		start_time = aimos_clock_read(); 
	}

   /*
    sort the elements in each rank 
    (with quicksort or CU_OddEvenNetworkSort)
    (idk)
   */
  #ifdef CUDA_MODE
    if (myrank == 0) {
      fprintf(stderr, "RANK 0: Doing Cuda Sort\n");
    }
    CU_Init();
    CU_OddEvenNetworkSort(data_start, data_end);
  #else
    if (myrank == 0) {
      fprintf(stderr, "RANK 0: Doing CPU Sort\n");
    }
    m_qsort(data_start, data_end);
  #endif

	MPI_Barrier(MPI_COMM_WORLD);


	int numSR = 4; //sends and requests
	MPI_Request reqs[numSR];
	MPI_Status stats[numSR];

	elem* mergeArray;
	elem* recv_arr;
    size_t* recv_size;



  //merge ranks together via merge sort
  for (size_t s = numranks/2; s >0 ; s>>1){
	  if (myrank < s){
		recv_arr= (elem*)malloc(nSize * sizeof(elem));
		recv_size = (sizt_t*)calloc(1, sizeof(size_t));

		rc = MPI_Irecv(recv_size , 1, MPI_Offset, myrank+s , 1, MPI_COMM_WORLD, reqs[2]);
		rc = MPI_Irecv(recv_arr, nSize, MPI_INT32_T, myrank+s , 0, MPI_COMM_WORLD, reqs[0]);
		rc = MPI_Waitall(numSR, reqs, stats );
	    mergeArray = merge(dataptr,  recv_arr);

		// asign dataptr = mergeArray, set size, free memory
		free(recv_size);
		free(dataptr);
		free(recv_arr);
		dataptr = mergeArray;
		nSize = sizeof(mergeArray)/sizeof(elem);
		data_start = dataptr;                 // start of (buffer) array
		data_end   = dataptr + nSize - 1;     // end of (buffer) array
	  }
	  else {
		//recv_size = &nSize;
		rc = MPI_Isend((void*)&nSize, 1, MPI_Offset, myrank-s , 1, MPI_COMM_WORLD, reqs[3]);
		rc = MPI_Isend(dataptr, nSize, MPI_INT32_T, myrank-s , 0, MPI_COMM_WORLD, reqs[1]);

		//terminate / snooze ... 
        
        //free(dataptr)
		//MPI_Finalize();
		//exit(1);
	  }
  }

	MPI_Barrier(MPI_COMM_WORLD);
	if (myrank == 0) {
		end_time = aimos_clock_read();
		duration = (end_time - start_time)/CLOCKS_PER_MSEC;
		fprintf(stderr, "TOTAL EXECUTION TIME: %llu MILLISECONDS\n", duration);
	
		printf("RANK(%d) FINISHED (MERGE) ALGORITHM numElems(%ld):", myrank, numElems());
		// why 100 
		if (numElems() > 100) {
			printf("Output too large, Omitting...\n");
		} else {
			for (elem* it = data_start; it <= data_end; ++it) {
				printf(" %d", *it);
			}
			printf("\n");
		}
	}


	free(dataptr);

	/* MPI Clean up */
	MPI_Finalize();
	return EXIT_SUCCESS;

}


elem* merge(elem* left, elem* right){
	size_t mergeSize = sizeof(left) + sizeof(right);
	elem* mergeArray = (elem *)malloc(mergeSize);
	
	size_t i=0, j = 0; k = 0;

	size_t l = sizeof(left)/sizeof(elem);
	size_t r = sizeof(right)/sizeof(elem);

	while (i < l && j < r){
		if (left[i] < right[j]){
			mergeArray[k] = left[i]
			i++;
		}
		else {
			mergeArray[k] = right[j];
			j++
		}
		k++;
	}

	//copy remaining elements
	while (i < l){
		mergeArray[k] = left[i];
		i++;
		k++;
	}
	while (j < r){
		mergeArray[k] = left[j];
		j++;
		k++;
	}

	return mergeArray;
}

size_t numElems() {
	return (data_end - data_start + 1);
}