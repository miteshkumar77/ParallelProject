#include <mpi/mpi.h>
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

/* Split array about a pivot value */
void split_array(elem** l_arr, size_t* l_sz, elem** r_arr, size_t* r_sz, elem pv);

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

	if (myrank == 0) {
		start_time = aimos_clock_read(); 
	}

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

	/* BEGIN PARALLEL SORT */
	MPI_Comm parent_comm = MPI_COMM_WORLD;
	int localRank = myrank;
	int localNumranks = numranks;
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
		elem consensusMedian = 0;
		if (localRank == 0) {
			/* Move all medians to the beginning of the array */
			int medianEnd = localNumranks;
			int medianStart = 0;
			for (; medianStart < medianEnd; medianStart += (localHaveElems[medianStart] > 0)) {
				if (localHaveElems[medianStart] == 0) {
					--medianEnd;
					swap(&localHaveElems[medianEnd], &localHaveElems[medianStart]);
					swap(&localMedians[medianEnd], &localMedians[medianStart]);
				}
			}

#ifdef DEBUG_MODE
			fprintf(stderr, "medianEnd(%d), localNumranks(%d)\n", medianEnd, localNumranks);
#endif

			consensusMedian = findKth(localMedians, localMedians + medianEnd - 1, medianEnd/2);
#ifdef DEBUG_MODE
			printf("RANK(%d) L_RANK(%d) MEDIANS:", myrank, localRank);
			for (int i = 0; i < localNumranks; ++i) {
				if (localHaveElems[i]) {
					printf(" %d", localMedians[i]);
				} else {
					printf(" _");
				}
			}
			printf("\n");
#endif

			free(localMedians);
			free(localHaveElems);
		}


		rc = MPI_Bcast(&consensusMedian, 1, MPI_INT32_T, 0, parent_comm);

		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Scatter(consensusMedian) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

#ifdef DEBUG_MODE	
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) CONSENSUS_MEDIAN(%d)\n", myrank, localRank, consensusMedian); 
#endif

		elem* split_point = partition(data_start, data_end, consensusMedian);
#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) SPLIT VAL(%d)\n", myrank, localRank, *split_point);
#endif

		elem* l_arr = NULL;
		elem* r_arr = NULL;
		size_t l_sz;
		size_t r_sz;

		split_array(&l_arr, &l_sz, &r_arr, &r_sz, *split_point);

#ifdef DEBUG_MODE		
		fprintf(stderr, "G_RANK(%d) L_RANK(%d): l_sz(%ld) + r_sz(%ld) = total_sz(%ld) <===> numElems(%ld)\n", myrank, localRank,
				l_sz, r_sz, l_sz + r_sz, numElems());  
#endif

		const int color = (localRank >= (localNumranks >> 1));
		const int key   = (localRank % (localNumranks >> 1));
		const int tag = 123;
		int src_rank;
		size_t send_size;
		elem* send_arr;
		size_t recv_size;
		elem* recv_arr;
		size_t keep_size;
		elem* keep_arr;

		if (color) { /* Right side */
			keep_size = r_sz;
			keep_arr = r_arr;
			send_size = l_sz;
			send_arr = l_arr;
			src_rank = localRank - (localNumranks >> 1);
		} else { /* Left side */
			keep_size = l_sz;
			keep_arr = l_arr;
			send_size = r_sz;
			send_arr = r_arr;
			src_rank = localRank + (localNumranks >> 1);
		}

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) SRC(%d)\n", myrank, localRank, src_rank);
#endif

		MPI_Status status_send;
		MPI_Status status_recv;
		MPI_Request request_send = MPI_REQUEST_NULL;
		MPI_Request request_recv = MPI_REQUEST_NULL;

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) sending send_size(%ld) to rank(%d)\n", myrank, localRank, send_size, src_rank);
#endif

		rc = MPI_Irecv(&recv_size, 1, MPI_UINT64_T, src_rank, tag, parent_comm, &request_recv);
		rc = MPI_Isend(&send_size, 1, MPI_UINT64_T, src_rank, tag, parent_comm, &request_send);

		rc = MPI_Wait(&request_send, &status_send);
		rc = MPI_Wait(&request_recv, &status_recv);

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) received recv_size(%ld) from rank(%d)\n", myrank, localRank, recv_size, src_rank);
#endif

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d): color(%d) recv_size(%ld)\n", myrank, localRank, color, recv_size);
#endif



		recv_arr = (elem*)calloc(recv_size, sizeof(elem));

		rc = MPI_Irecv(recv_arr, recv_size, MPI_INT32_T, src_rank, tag, parent_comm, &request_recv);
		rc = MPI_Isend(send_arr, send_size, MPI_INT32_T, src_rank, tag, parent_comm, &request_send);



		rc = MPI_Wait(&request_send, &status_send);
		rc = MPI_Wait(&request_recv, &status_recv);



		free(send_arr);
		free(dataptr);




		elem* new_arr = calloc(keep_size + recv_size, sizeof(elem));
		memcpy(new_arr, keep_arr, keep_size * sizeof(elem));
		memcpy(new_arr + keep_size, recv_arr, recv_size * sizeof(elem));

#ifdef DEBUG_MODE		
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) FINISHED EXCHANGING\n", myrank, localRank);

		fprintf(stderr, "G_RANK(%d) L_RANK(%d) KEEP_SIZE(%ld)", myrank, localRank, keep_size);
		printf("G_RANK(%d) keep_data:", myrank);
		for (int i = 0; i < keep_size; ++i) {
			printf(" %d", keep_arr[i]);
		}
		printf("\nG_RANK(%d) recv_data:", myrank);
		for (int i = 0; i < recv_size; ++i) {
			printf(" %d", recv_arr[i]);
		}
		printf("\nG_RANK(%d) final_data:", myrank);
		for (int i = 0; i < recv_size + keep_size; ++i) {
			printf(" %d", new_arr[i]);
		}

		printf("\n");
#endif	
		free(recv_arr);
		free(keep_arr);
		dataptr = new_arr;
		data_start = new_arr;
		data_end = new_arr + recv_size + keep_size - 1;
		

		MPI_Barrier(parent_comm);
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

	m_qsort(data_start, data_end);

	MPI_Barrier(MPI_COMM_WORLD);
	/* END PARALLEL SORT */

	size_t out_size = numElems() * sizeof(elem);

	size_t* fsums = NULL;

	if (myrank == 0) {
		fsums = calloc(numElems() + 1, sizeof(size_t));
		if (fsums == NULL) {
			fprintf(stderr, "ERROR RANK(%d): calloc() failed\n", myrank);
			MPI_Abort(MPI_COMM_WORLD, 0);
		}
	}

	rc = MPI_Gather(&out_size, 1, MPI_UINT64_T, fsums + 1, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
	if (rc != MPI_SUCCESS) {
		MPI_Error_string(rc, errorStr, &errorlen);
		fprintf(stderr, "ERROR RANK(%d): MPI_Gather() failed with error code(%d): %s\n", myrank, rc, errorStr);
		MPI_Abort(MPI_COMM_WORLD, rc);
	}
	if (myrank == 0) {	
		for (int i = 1; i <= numranks; ++i) {
			fsums[i] += fsums[i - 1];
		}
	}

	size_t writeAt;
	rc = MPI_Scatter(fsums, 1, MPI_UINT64_T, &writeAt, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);


	if (rc != MPI_SUCCESS) {
		MPI_Error_string(rc, errorStr, &errorlen);
		fprintf(stderr, "ERROR RANK(%d): MPI_Scatter() failed with error code(%d): %s\n", myrank, rc, errorStr);
		MPI_Abort(MPI_COMM_WORLD, rc);
	}

	free(fsums);
	
	fprintf(stderr, "RANK(%d) writing (%ld) bytes at offset (%ld)\n", myrank, out_size, writeAt);
	MPI_Barrier(MPI_COMM_WORLD);
	
	if (myrank == 0) {
		end_time = aimos_clock_read();
		duration = (end_time - start_time)/CLOCKS_PER_MSEC;
		fprintf(stderr, "TOTAL EXECUTION TIME: %llu MILLISECONDS\n", duration);
	}
				


#ifdef DEBUG_MODE
	printf("RANK(%d) FINISHED ALGORITHM numElems(%ld):", myrank, numElems());
	for (elem* it = data_start; it <= data_end; ++it) {
		printf(" %d", *it);
	}
	printf("\n");
#endif



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



void split_array(elem** l_arr, size_t* l_sz, elem** r_arr, size_t* r_sz, elem pv) {
	size_t ls = 0;
	size_t rs = 0;
	
	for (elem* it = data_start; it <= data_end; ++it) {
		if (cmp(it, &pv) == 1) {
			++rs;
		} else {
			++ls;
		}
	}
	
	*l_arr = calloc(ls, sizeof(elem));
	*r_arr = calloc(rs, sizeof(elem));
	elem* l_put = *l_arr;
	elem* r_put = *r_arr;
	for (elem* it = data_start; it <= data_end; ++it) {
		if (cmp(it, &pv) == 1) {
			*(r_put++) = *it;
		} else {
			*(l_put++) = *it;
		}
	}

	*l_sz = ls;
	*r_sz = rs;

}



























