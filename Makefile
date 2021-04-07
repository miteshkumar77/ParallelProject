project:
	mpicc -Wall -Werror parallel-qsort.c -o project.out -std=c99

generator:
	gcc -Wall -Werror data_gen.c -o generator.out -std=c99

project_dbg:
	mpicc -Wall -Werror parallel-qsort.c -o project_dbg.out -D DEBUG_MODE -std=c99

generator_dbg:
	gcc -Wall -Werror data_gen.c -o generator_dbg.out -D DEBUG_MODE -std=c99


project-loc:
	mpicc -Wall -Werror parallel-qsort.c -o project.out -std=c99 -D LOCAL

generator-loc:
	gcc -Wall -Werror data_gen.c -o generator.out -std=c99 -D LOCAL

project_dbg-loc:
	mpicc -Wall -Werror parallel-qsort.c -o project_dbg.out -D DEBUG_MODE -std=c99 -D LOCAL

generator_dbg-loc:
	gcc -Wall -Werror data_gen.c -o generator_dbg.out -D DEBUG_MODE -std=c99 -D LOCAL
