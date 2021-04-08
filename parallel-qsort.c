#include <mpi/mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "./serial_sort.h"
#include "./filereader.h"

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
			fprintf(stderr, "medianEnd(%d), localNumranks(%d)\n", medianEnd, localNumranks);
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
			// free(localMedians);
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
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) SPLIT VAL(%d)\n", myrank, localRank, *split_point);

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
	
		rc = MPI_Irecv(&recv_size, 1, MPI_UINT64_T, src_rank, tag, parent_comm, &request_recv);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Irecv(size) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Isend(&send_size, 1, MPI_UINT64_T, src_rank, tag, parent_comm, &request_send);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Isend(size) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Wait(&request_send, &status_send);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Wait(send_size) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Wait(&request_recv, &status_recv);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Wait(recv_size) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d): color(%d) recv_size(%ld)\n", myrank, localRank, color, recv_size);
#endif

		
		recv_arr = (elem*)calloc(recv_size, sizeof(elem));

		if (recv_arr == NULL) {
			fprintf(stderr, "ERROR RANK(%d): calloc() failed\n", myrank);
			MPI_Abort(MPI_COMM_WORLD, 0);
		}

		rc = MPI_Irecv(recv_arr, recv_size, MPI_INT32_T, src_rank, tag, parent_comm, &request_recv);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_IRecv(arr) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Isend(send_arr, send_size, MPI_INT32_T, src_rank, tag, parent_comm, &request_send);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Isend(arr) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Wait(&request_send, &status_send);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Wait(send_arr) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}

		rc = MPI_Wait(&request_recv, &status_recv);
		if (rc != MPI_SUCCESS) {
			MPI_Error_string(rc, errorStr, &errorlen);
			fprintf(stderr, "ERROR RANK(%d): MPI_Wait(recv_arr) failed with error code(%d): %s\n", myrank, rc, errorStr);
			MPI_Abort(MPI_COMM_WORLD, rc);
		}
#ifdef DEBUG_MODE
		fprintf(stderr, "G_RANK(%d) L_RANK(%d) FINISHED EXCHANGING\n", myrank, localRank);
#endif

		// free(send_arr);
		fprintf(stderr, "RANK(%d) FREED SEND_ARR\n", myrank);
		// free(dataptr);
		fprintf(stderr, "RANK(%d) FREED DATA_PTR\n", myrank);

		keep_arr = realloc(keep_arr, keep_size + recv_size);
		fprintf(stderr, "RANK(%d) REALLOCED (%ld)\n", myrank, keep_size + recv_size);
		keep_arr = (elem*)memcpy((void*)(keep_arr + keep_size), (void*)recv_arr, (size_t)(recv_size * sizeof(elem)));  
		fprintf(stderr, "RANK(%d) MEMCPYD\n", myrank);

	//	free(recv_arr);
		fprintf(stderr, "RANK(%d) FREED RECV_ARR\n", myrank);

		dataptr = keep_arr;
		fprintf(stderr, "RANK(%d) DATAPTR SET\n", myrank);
		data_start = keep_arr;
		fprintf(stderr, "RANK(%d) DATA_START SET\n", myrank);
		data_end = keep_arr + recv_size + keep_size - 1;
		
		fprintf(stderr, "RANK(%d) DATA_END SET\n", myrank);

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
	
	printf("RANK(%d) FINISHED ALGORITHM numElems(%ld):", myrank, numElems());
	for (elem* it = data_start; it <= data_end; ++it) {
		printf(" %d", *it);
	}
	printf("\n");

	// free(dataptr);

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







































