/*
 * Includes code to generate a random set of 
 * numbers and writes them to a file. Might 
 * want to parallelize this later for larger
 * data sets
 */
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "./serial_sort.h"

#define BUFF_SZ 1024

/* lower bound */
int lb;

/* upper bound */
int ub;

/* number of numbers */
ssize_t n;

/* file path */
char* fpath;

/* file descriptor */
int fd;

/* return code */
int rc;

/* write buffer */
elem* wbuff;


ssize_t min(ssize_t a, ssize_t b) { return a < b ? a : b; }


/* usage [ executable ] [ lowest number ] [ highest number ] [ number of points ] [ fpath ] */
int main(int argc, char** argv) {
	if (argc != 5) {
		fprintf(stderr, "ERROR: invalid argument(s)\n");
		fprintf(stderr, "USAGE: [ executable ] [ lowest number ] [ highest number ] [ number of points ] [ fpath ]\n");
		return EXIT_FAILURE;
	}
	
	lb    = atoi(argv[1]);
	ub    = atoi(argv[2]);
	n     = atoi(argv[3]);
	fpath = argv[4];
	
	fd = open(fpath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd == -1) {
		perror("ERROR: open() failed");
		return EXIT_FAILURE;
	}

#ifdef DEBUG_MODE	
	printf("File Descriptor: %d\n", fd);
#endif
	ssize_t num_elems;
	wbuff = (elem *) malloc((BUFF_SZ * sizeof(elem))); 

#ifdef DEBUG_MODE	
	printf("WRITE:");
#endif
	for (int i = 0; i < n; i += BUFF_SZ) {
		num_elems = min(BUFF_SZ, n - i);	
		for (int j = 0; j < num_elems; ++j) {
			wbuff[j] = rand() % (ub - lb + 1) + lb;
			
#ifdef DEBUG_MODE	
			printf(" %d", wbuff[j]);
#endif
		}
		rc = write(fd, wbuff, num_elems * sizeof(elem));
		if (rc == -1) {
			perror("ERROR: write() failed");
			free(wbuff);
			return EXIT_FAILURE;
		}
	}

#ifdef DEBUG_MODE	
	printf("\n");
#endif
	rc = close(fd);
	if (rc == -1) {
		perror("ERROR: close() failed");
		free(wbuff);
		return EXIT_FAILURE;
	}
#ifdef DEBUG_MODE	
	rc = open(fpath, O_RDONLY);
	if (fd == -1) {
		perror("ERROR: open() failed");
		free(wbuff);
		return EXIT_FAILURE;
	}
	
	printf("READ: ");
	for (int i = 0; i < n; i += BUFF_SZ) {
		num_elems = min(BUFF_SZ, n - i);
		rc = read(fd, wbuff, num_elems * sizeof(elem));
		if (rc == -1) {
			perror("ERROR: read() failed");
			free(wbuff);
			return EXIT_FAILURE;
		}
		for (int j = 0; j < num_elems; ++j) {
			printf(" %d", wbuff[j]);
		}
	}
	printf("\n");
#endif
	free(wbuff);
	return EXIT_SUCCESS;
}











