#!/bin/bash -x

module load xl_r
module load spectrum-mpi
module load cuda/10.2
module load cmake

srun hostname -s > /tmp/hosts.$SLURM_JOB_ID

if [ "x$SLURM_NPROCS" = "x" ]
then
  if [ "x$SLURM_NTASKS_PER_NODE" = "x" ]
  then
    SLURM_NTASKS_PER_NODE=1
  fi
  SLURM_NPROCS=`expr $SLURM_JOB_NUM_NODES \* $SLURM_NTASKS_PER_NODE`
fi

./generator.out 0 10000 2000000 exponential /gpfs/u/home/PCPA/PCPAgjnn/scratch/tempfile.txt

taskset --cpu-list 0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92\
,96,100,104,108,112,116,120,124,128,132,136,140,144,148,152,156 mpirun -hostfile /tmp/h\
osts.$SLURM_JOB_ID -np $SLURM_NPROCS\
  /gpfs/u/home/PCPA/PCPAgjnn/ParallelProject/project.out /gpfs/u/home/PCPA/PCPAgjnn/scratch/tempfile.txt fakepath.txt >/gpfs/u/home/PCPA/PCPAgjnn/ParallelProject/ExponentialWS/2m-project

taskset --cpu-list 0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76,80,84,88,92\
,96,100,104,108,112,116,120,124,128,132,136,140,144,148,152,156 mpirun -hostfile /tmp/h\
osts.$SLURM_JOB_ID -np $SLURM_NPROCS\
  /gpfs/u/home/PCPA/PCPAgjnn/ParallelProject/parallel-qsort.exe /gpfs/u/home/PCPA/PCPAgjnn/scratch/tempfile.txt fakepath.txt >/gpfs/u/home/PCPA/PCPAgjnn/ParallelProject/ExponentialWS/2m-parallelQsort

  
rm /gpfs/u/home/PCPA/PCPAgjnn/scratch/tempfile.txt
rm /tmp/hosts.$SLURM_JOB_ID