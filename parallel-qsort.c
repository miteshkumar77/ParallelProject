#include <mpi/mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "./serial_sort.h"
#include "./filereader.h"

#define IS_LEADER(_rank, _pool) _rank % _pool == 0 

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

/* Number of bits in a 32 bit integer
 * If bitCount(n) = 1, then n is a power of 2
 */
int bitCount(int num);


/* Get the number of elements 
 * this rank currently holds
 */
size_t numElems();

/* Get the median of the data set */
elem getMedian();


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
	data_start = dataptr;
	data_end   = dataptr + nSize - 1;
	
	if (nSize == 0) {
		fprintf(stderr, "ERROR rank(%d): More ranks than elements in the input.\n", myrank);

		MPI_Abort(MPI_COMM_WORLD, 0);
		exit(EXIT_FAILURE);
	}

	m_qsort(dataptr, dataptr + nSize - 1);

	MPI_Comm parent_comm = MPI_COMM_WORLD;
	int localRank = myrank;
	int localNumranks = numranks;
	/* BEGIN PARALLEL SORT */
	MPI_Barrier(MPI_COMM_WORLD);
	while(localNumranks > 1) {

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) G_NUMRANKS(%d) L_RANK(%d) L_NUMRANKS(%d)\n", myrank, numranks, localRank, localNumranks);
#endif
		
		int* localHaveElems = NULL;
		elem* localMedians = NULL;
		
		int ownHasElems = (numElems() > 0);
		elem ownLocalMedian = 0;
		if (ownHasElems) {
			ownLocalMedian = getMedian();
		}

		if (localRank == 0) { /* LEADER */
#ifdef DEBUG_MODE
			fprintf(stderr, "LEADER RANK(%d) L_RANK(%d)\n", myrank, localRank);
#endif
			
			localHaveElems = (int*)calloc(localNumranks, sizeof(int));
			localMedians = (elem*)calloc(localNumranks, sizeof(elem));
		}
		
		rc = MPI_Gather(&ownHasElems, 1, MPI_INT32_T, localHaveElems, 1, MPI_INT32_T, 0, parent_comm);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Gather(HasElems) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Gather(&ownLocalMedian, 1, MPI_INT32_T, localMedians, 1, MPI_INT32_T, 0, parent_comm);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Gather(localMedians) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}
#ifdef DEBUG_MODE
		if (localRank == 0) {
			printf("RANK(%d) L_RANK(%d) MEDIANS:", myrank, localRank);
			for (int i = 0; i < localNumranks; ++i) {
				if (localHaveElems[i]) {
					printf(" %d", localMedians[i]);
				} else {
					printf(" _");
				}
			}
			printf("\n");
		}
#endif
		elem consensusMedian = 0;
		if (localRank == 0) {
			consensusMedian = findKth(localMedians, localMedians + localNumranks - 1, localNumranks/2);
		}
		elem recvConsensusMedian;
		
	
		rc = MPI_Scatter(&consensusMedian, 1, MPI_INT32_T, &recvConsensusMedian, 1, MPI_INT32_T, 0, parent_comm); 

		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Scatter(consensusMedian) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

#ifdef DEBUG_MODE	
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) CONSENSUS_MEDIAN(%d)\n", myrank, localRank, recvConsensusMedian); 
#endif

		MPI_Barrier(parent_comm);
		int color = (localRank >= (localNumranks >> 1));
		int key   = (localRank % (localNumranks >> 1));
		rc = MPI_Comm_split(parent_comm, color, key, &parent_comm);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Comm_split() failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}
		
		rc = MPI_Comm_rank(parent_comm, &localRank);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Comm_rank() failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Comm_size(parent_comm, &localNumranks);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Comm_size() failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

	}
	




	MPI_Barrier(MPI_COMM_WORLD);

	/* END PARALLEL SORT */

	free(dataptr);

	/* MPI Clean up */
	MPI_Finalize();
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




size_t numElems() {
	return (data_end - data_start + 1);
}

elem getMedian() {
	return findKth(data_start, data_end, numElems()/2);
}










































