#include <mpi/mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "./serial_sort.h"
#include "./filereader.h"

#define IS_LEADER(_rank, _pool) _rank % _pool == 0 
/* own process rank, total number of ranks */
int myrank, numranks;

/* Group consisting of all ranks */
MPI_Group world_group;

/* Store data array */
elem* dataptr;

/* Path to read from */
char* frpath;

/* Path to write to */
char* fwrpath;


/* Number of bits in a 32 bit integer
 * If bitCount(n) = 1, then n is a power of 2
 */
int bitCount(int num);

/**
 * Parallel Sort Algorithm
 */
int main(int argc, char** argv) {	
	/* MPI Initialization */
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &numranks);
	MPI_Comm_group(MPI_COMM_WORLD, &world_group);

	/* Verify that numranks is a power of 2 */
	if (bitCount(numranks) != 1) {
		fprintf(stderr, "ERROR rank(%d): number of ranks = %d wasn't a power of 2, total 1 bits: %d, should be 1\n", myrank, numranks, bitCount(numranks));
		return EXIT_FAILURE;
	}


	/* Input argument validation */
	if (argc != 3) {
		fprintf(stderr, "ERROR rank(%d): invalid arguments\n", myrank);
		fprintf(stderr, "USAGE: %s <frpath> <fwrpath>\n", *argv); 
		return EXIT_FAILURE;
	}

	/* Set fpath to read from */
	frpath = *(argv + 1);

	MPI_Offset bytes_read = readfile(myrank, numranks, &dataptr, frpath, MPI_COMM_WORLD); 
	MPI_Offset nSize = bytes_read/sizeof(elem);
	
	if (nSize < numranks) {
		fprintf(stderr, "ERROR rank(%d): More ranks than elements in the input.\n", myrank);
		exit(EXIT_FAILURE);
	}

#ifdef DEBUG_MODE	
	printf("ORIGINAL:");
	for (MPI_Offset i = 0; i < nSize; ++i) {
		printf(" %d", *(dataptr + i));
	}
	printf("\n");
#endif



	m_qsort(dataptr, dataptr + nSize - 1);
//	m_bubble_sort(dataptr, dataptr + nSize - 1);
#ifdef DEBUG_MODE	
	/* Validate sort */
	for (MPI_Offset i = 0; i + 1 < nSize; ++i) {
		if ((*(dataptr + i)) > *(dataptr + i + 1)) {
			printf("ERROR rank(%d): Array not sorted...", myrank);
			return EXIT_FAILURE;
		}
	}

	printf("AFTER SORT:");
	for (MPI_Offset i = 0; i < nSize; ++i) {
		printf(" %d", *(dataptr + i));
	}
	printf("\n");
#endif
	
	/* BEGIN PARALLEL SORT */
	MPI_Barrier(MPI_COMM_WORLD);

	/* part: offset of the node that we have to send to / recv from */
	/* pool: size of the current comm group */
	int part = numranks >> 1;
	int pool = numranks;
	for (; part > 0; part >>= 1, pool >>= 1) {
		/* create a ranks array for this comm group */
		int* ranks = malloc(pool * sizeof(int));
		if (ranks == NULL) {
			fprintf(stderr, "ERROR rank(%d): malloc() failed\n", myrank);
			exit(EXIT_FAILURE);
		}
		/* the starting rank that is part of the comm group for this iteration */
		int r = (myrank / pool) * pool;
#ifdef DEBUG_MODE
		printf("rank(%d) part(%d) pool(%d): STARTRANK = %d\n", myrank, part, pool, r);
#endif
		int idx = 0;
		for (; idx < pool; ++r, ++idx) {
			ranks[idx] = r;
		}

		MPI_Comm comm_pool;
		

		/* LEADER is the first rank of the pool */
		if (IS_LEADER(myrank, pool)) {
#ifdef DEBUG_MODE
			printf("LEADER: rank(%d) part(%d) pool(%d)\n", myrank, part, pool);
#endif
		}
#ifdef DEBUG_MODE
		printf("GROUP rank(%d) part(%d) pool(%d):", myrank, part, pool);
		for (int i = 0; i < pool; ++i) {
			printf(" %d", ranks[i]);
		}
		printf("\n");
#endif

		free(ranks);
		MPI_Barrier(MPI_COMM_WORLD);
	}
	
	/* END PARALLEL SORT */
	free(dataptr);
	
	return EXIT_SUCCESS;
}


int bitCount(int num) {
	__int32_t bit = 1;
	int ans = 0;
	for (int i = 0; i < sizeof(num) * 4; ++i, bit <<= 1) {
		ans += ((bit & num) != 0);
	}
	return ans;
}














































