## ParallelProject

https://www.overleaf.com/2843638255vbbtgrffwdbn


9 pages double column, single spacing, 10 point font with more performance graphs and more references


## Commands

read generator
```
./generator.out 0 10000 1000000 normal /gpfs/u/home/PCPA/PCPAgjnn/scratch/datafile.txt
ls ~/scratch/datafile.txt
hexdump ~/scratch/datafile.txt
 ```

```
ssh dcsfen02
module load xl_r spectrum-mpi cuda/10.2
make generator
make project
make project-cuda
salloc -N 1 --partition=rpi --gres=gpu:4 -t 60

```

# for weak scaling
```
sbatch -N 1 --ntasks-per-node=1 --partition=dcs --gres=gpu:6 -t 10 ./benchmark1.sh
sbatch -N 1 --ntasks-per-node=2 --partition=dcs --gres=gpu:6 -t 10 ./benchmark2.sh
sbatch -N 1 --ntasks-per-node=4 --partition=dcs --gres=gpu:6 -t 10 ./benchmark4.sh
sbatch -N 1 --ntasks-per-node=8 --partition=dcs --gres=gpu:6 -t 10 ./benchmark8.sh
sbatch -N 1 --ntasks-per-node=16 --partition=dcs --gres=gpu:6 -t 10 ./benchmark16.sh
```
# for strongscaling
```
sbatch -N 1 --ntasks-per-node=1 --partition=dcs --gres=gpu:6 -t 10 ./benchmark.sh
sbatch -N 1 --ntasks-per-node=2 --partition=dcs --gres=gpu:6 -t 10 ./benchmark.sh
sbatch -N 1 --ntasks-per-node=4 --partition=dcs --gres=gpu:6 -t 10 ./benchmark.sh
sbatch -N 1 --ntasks-per-node=8 --partition=dcs --gres=gpu:6 -t 10 ./benchmark.sh
sbatch -N 1 --ntasks-per-node=16 --partition=dcs --gres=gpu:6 -t 10 ./benchmark.sh
```

