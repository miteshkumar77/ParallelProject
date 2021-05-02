project:
	mpicc -Wall -Werror parallel-qsort.c -o project.out -std=c99

generator:
	gcc -Wall -Werror data_gen.c -o generator.out -std=c99 -lm

project-cuda:
	mpixlc -g parallel-qsort.c -c -o parallel-qsort.o
	nvcc -g -G -arch=sm_70 cuda_sort.cu -c -o cuda_sort.o
	mpicc -g parallel-qsort.o cuda_sort.o -o parallel-qsort.exe \
		-L/usr/local/cuda-10.2/lib64/ -lcudadevrt -lcudart -lstdc++