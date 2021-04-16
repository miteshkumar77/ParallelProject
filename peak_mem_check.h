#ifndef PEAK_MEM_CHECK_H
#define PEAK_MEM_CHECK_H

#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/wait.h>
#include <linux/limits.h>
#include <string.h>


void peak_mem_check(pid_t r_pid) {
	int rc;
	int p[2];

	int* p_read = p;
	int* p_write = p + 1;

	pid_t t_pid = fork();
	if (t_pid == -1) {
		perror("ERROR: fork() failed");
		exit(EXIT_FAILURE);
	}
	if (t_pid == 0) {

		rc = pipe(p);
		if (rc == -1) {
			perror("ERROR: pipe() failed");
			exit(EXIT_FAILURE);
		}

		pid_t f_pid = fork();
		if (f_pid == -1) {
			perror("ERROR: fork() failed");
			exit(EXIT_FAILURE);
		}

		if (f_pid == 0) { // Child
		
			rc = close(*p_read);
			if (rc == -1) {
				perror("ERROR: close() failed");
				exit(EXIT_FAILURE);
			}
	
			rc = dup2(*p_write, 1);
			if (rc == -1) {
				perror("ERROR: dup2() failed");
				exit(EXIT_FAILURE);
			}
		
			char fpath[PATH_MAX];
			sprintf(fpath, "/proc/%d/status", r_pid);
			execl("/usr/bin/cat", "cat", fpath, NULL); 
			perror("ERROR: execl() failed");
			exit(EXIT_FAILURE);
		} else { // Parent
			close(*p_write);
			dup2(*p_read, 0);
			execl("/usr/bin/grep", "grep", "VmPeak", NULL);
			perror("ERROR: execl() failed");
			exit(EXIT_FAILURE);
		}
	}
	rc = waitpid(-1, NULL, 0);
	if (rc == -1) {
		perror("ERROR: waitpid() failed");
		exit(EXIT_FAILURE);
	}
	waitpid(-1, NULL, 0);
	if (rc == -1) {
		perror("ERROR: waitpid() failed");
		exit(EXIT_FAILURE);
	}
}








#endif
