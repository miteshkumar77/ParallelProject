/* Compile with -D DEBUG_MODE for debug output.
 *
 * Contains methods to:
 *
 * -- read elements from a file in 
 * binary form. Uses the type for element in 
 * "serial_sort.h". 
 * Each rank reads an approximately even number
 * of points. Last rank may have more or less.
 * (tested a bit)
 *
 *
 * -- write elements to a file in binary form.
 *    (not tested yet)
 */
#include <mpi.h>
#include "serial_sort.h"

MPI_Offset readfile(int myrank, int numranks, elem** dataptr, char* fname, MPI_Comm fcomm) {

	char error_str[MPI_MAX_ERROR_STRING];
	int errlen;
	int rc;

	MPI_File fh;
	rc = MPI_File_open(fcomm, fname, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_open() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}		

	MPI_Offset fsize;
	rc = MPI_File_get_size(fh, &fsize);
	
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_get_size() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}		
	MPI_Offset delta = ( ( ( fsize / sizeof( elem ) ) ) / numranks ) * sizeof ( elem );
	MPI_Offset offset = delta * myrank;
	MPI_Offset numrd = myrank + 1 == numranks ? fsize - offset : delta;

#ifdef DEBUG_MODE
	printf("FSIZE     rank(%d): %lld\n", myrank, fsize);
	printf("NUM ELEMS rank(%d): %lld\n", myrank, (fsize / sizeof(elem)));
  printf("DELTA     rank(%d): %lld\n", myrank, delta );
	printf("OFFSET    rank(%d): %lld\n", myrank, offset );
	printf("NUMRD     rank(%d): %lld\n", myrank, numrd );
	printf("NUM ELEMS rank(%d): %lld\n", myrank, numrd / sizeof(elem));
#endif

	*dataptr = (elem *)malloc((size_t)numrd);
	if (dataptr == NULL) {
		fprintf(stderr, "ERROR rank(%d): malloc() failed.", myrank);
	}
	rc = MPI_File_read_at(fh, offset, *dataptr, numrd, MPI_CHAR, MPI_STATUS_IGNORE);
	
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_read_at(numrd: %lld) failed with error code (%d): %s", myrank, numrd, rc, error_str);
		exit(EXIT_FAILURE);
	}		

	rc = MPI_File_close(&fh);
	
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_close() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}
	return numrd;
}

void writefile(int myrank, int numranks, MPI_Offset startwr, MPI_Offset numwr, const elem* dataptr, char* fname, MPI_Comm fcomm) {
	
	char error_str[MPI_MAX_ERROR_STRING];
	int errlen;
	int rc;

	MPI_File fh;
	rc = MPI_File_open(fcomm, fname, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_open() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}		

	rc = MPI_File_write_at(fh, startwr, dataptr, numwr, MPI_CHAR, MPI_STATUS_IGNORE);
	
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_write_at() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}		
	
	rc = MPI_File_close(&fh);
	
	if (rc != 0) {
		MPI_Error_string(rc, error_str, &errlen);
		fprintf(stderr, "ERROR rank(%d): MPI_File_close() failed with error code (%d): %s", myrank, rc, error_str);
		exit(EXIT_FAILURE);
	}	
}









































