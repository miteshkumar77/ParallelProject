#include "./peak_mem_check.h"


int main(int argc, char** argv) {
	
	if (argc != 2) {
		fprintf(stderr, "INVALID ARGS\n");
		return EXIT_FAILURE;
	}
	int r_pid = atoi(*(argv + 1));
	peak_mem_check(r_pid);
	return EXIT_SUCCESS;
}
