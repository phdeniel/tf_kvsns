#!bin/bash

mod_path=/scratch/modules/Stages/2020a/

#Module use:
module use $mod_path/modules/all
module use $mod_path/modules/all/Compiler/GCCcore/7.5.0
module use $mod_path/modules/all/Core

#Module load
#module remove ohpc prun gnu mpich
module purge
module load Bazel
module load Python/3.7.7
module load GCC/7.5.0 OpenMPI
module load SciPy-Stack
module load h5py
module load cuDNN
module load NCCL
module load git
module load SWIG

# Environment variables
export CC_OPT_FLAGS="-O2 -ftree-vectorize -march=native -fno-math-errno"
export CUDA_TOOLKIT_PATH="$mod_path/software/CUDA/10.2.89"
export CUDNN_INSTALL_PATH="$mod_path/software/cuDNN/7.6.5.32-CUDA-10.2.89"
export GCC_HOST_COMPILER_PATH="$mod_path/software/GCCcore/7.5.0/bin/gcc"
export MPI_HOME=""
export NCCL_INSTALL_PATH="$mod_path/software/NCCL/2.5.6-GCCcore-7.5.0-CUDA-10.2.89"
export PYTHON_BIN_PATH="$mod_path/software/Python/3.7.7-GCCcore-7.5.0/bin/python"
export PYTHON_LIB_PATH="$mod_path/software/TensorFlow/1.15.2-GCC-7.5.0-GPU-Python-3.7.7/lib/python3.7/site-packages"
export TF_CUBLAS_VERSION="10.2.2"
export TF_CUDA_CLANG="0"
export TF_CUDA_COMPUTE_CAPABILITIES="3.5,3.7"
export TF_CUDA_PATHS="$mod_path/software/CUDA/10.2.89"
export TF_CUDA_VERSION="10.2"
export TF_CUDNN_VERSION="7.6.5"
export TF_ENABLE_XLA="0"
export TF_NCCL_VERSION="2.5.6"
export TF_NEED_AWS="0"
export TF_NEED_CUDA="1"
export TF_NEED_GCP="0"
export TF_NEED_GDR="0"
export TF_NEED_HDFS="0"
export TF_NEED_JEMALLOC="1"
export TF_NEED_KAFKA="0"
export TF_NEED_MPI="0"
export TF_NEED_OPENCL="0"
export TF_NEED_OPENCL_SYCL="0"
export TF_NEED_S3="0"
export TF_NEED_TENSORRT="0"
export TF_NEED_VERBS="0"
