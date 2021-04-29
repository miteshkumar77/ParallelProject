/*
 * Includes code to generate a random set of 
 * numbers and writes them to a file. Might 
 * want to parallelize this later for larger
 * data sets
 */

#define _XOPEN_SOURCE
#include <math.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include "./serial_sort.h"

#define BUFF_SZ 1024

/* lower bound */
elem lb;

/* upper bound */
elem ub;

/* number of numbers */
ssize_t n;

/* file path */
char* fpath;

/* distribution shape */
char* distribution;

/* file descriptor */
int fd;

/* return code */
int rc;

/* write buffer */
elem* wbuff;

/* normal constants */
elem norm2;
int done = 0;

/* exponential constants */
const double lambda = 0.01;

ssize_t min(ssize_t a, ssize_t b) { return a < b ? a : b; }

elem (*genPtr)();

elem get_uniform() {
	return ((elem)rand()) % (ub - lb + 1) + lb;
}	

elem get_normal() {
#if 0
	if (done) {
		done = 0;
		return norm2;
	}

	double x,y,rsq,f;
	do {
		x = 2.0 * rand() / (double)RAND_MAX - 1.0;
    y = 2.0 * rand() / (double)RAND_MAX - 1.0;
		rsq = x * x + y * y;
	} while (rsq >= 1. || rsq == 0.);
	f = sqrt(-2.0 * log(rsq)/rsq);
	norm2 = (elem)((y * f * (double)(ub - lb)) + ((double)(lb) + ((double)(ub - lb))/2));
	done = 1;
	return (elem)((x * f * (double)(ub - lb)) + ((double)(lb) + ((double)(ub - lb))/2));
#endif

	double x = (double)rand() / RAND_MAX;
	double y = (double)rand() / RAND_MAX;
	double z = sqrt(-2 * log(x)) * cos(2 * M_PI * y);
	double lb_dbl = lb;
	double ub_dbl = ub;
	return (elem)(z * (ub_dbl - lb_dbl) + (lb_dbl + (ub_dbl - lb_dbl)/2));
}

elem get_exp() {
	double res = INT_MAX;
	while (res > (double)ub) {
		res = lb + floor(-log(drand48()) / lambda); 
	}
	return (elem)res;
}

/* usage [ executable ] [ lowest number ] [ highest number ] [ number of points ] [ distribution ] [ fpath ] */
int main(int argc, char** argv) {
	if (argc != 6) {
		fprintf(stderr, "ERROR: invalid argument(s)\n");
		fprintf(stderr, "USAGE: [ executable ] [ lowest number ] [ highest number ] [ number of points ] [ distribution ] [ fpath ]\n");
		return EXIT_FAILURE;
	}
	
	srand48(time(0));
	lb    = atoi(argv[1]);
	ub    = atoi(argv[2]);
	n     = atoi(argv[3]);
	distribution = argv[4];
	fpath = argv[5];
	
	if (0 == strcmp(distribution, "uniform")) {
		genPtr = &get_uniform;
	} else if (0 == strcmp(distribution, "normal")) {
		genPtr = &get_normal;
	} else if (0 == strcmp(distribution, "exponential")) {
		genPtr = &get_exp;
	} else {
		fprintf(stderr, "ERROR: unknown distribution\n");
		return EXIT_FAILURE;
	}


	
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
			wbuff[j] = (*genPtr)();
			
#ifdef DEBUG_MODE	
			printf(" %d,", wbuff[j]);
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











