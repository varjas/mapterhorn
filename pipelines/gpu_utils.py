"""
GPU Acceleration Utilities for Mapterhorn Pipeline

This module provides GPU acceleration using CuPy with automatic CPU fallback.
If GPU is unavailable or CuPy is not installed, all operations gracefully
fall back to CPU using NumPy/SciPy.

Key Features:
- Automatic GPU detection and initialization
- Transparent CPU fallback
- Memory-efficient processing for large datasets
- Drop-in replacement for NumPy/SciPy operations
"""

import sys
import numpy as np
from scipy import ndimage as cpu_ndimage

# Try to import GPU libraries
GPU_AVAILABLE = False
try:
    import cupy as cp
    from cupyx.scipy import ndimage as gpu_ndimage

    GPU_AVAILABLE = True
    print("GPU acceleration available via CuPy", file=sys.stderr)
except ImportError:
    print("CuPy not found - using CPU for all operations", file=sys.stderr)
    cp = None
    gpu_ndimage = None


class GPUContext:
    """Context manager for GPU operations with automatic memory management"""

    def __init__(self, use_gpu=True):
        self.use_gpu = use_gpu and GPU_AVAILABLE
        self.mempool = None

    def __enter__(self):
        if self.use_gpu:
            # Clear GPU memory pool to ensure clean state
            self.mempool = cp.get_default_memory_pool()
            self.mempool.free_all_blocks()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.use_gpu:
            # Free GPU memory
            self.mempool.free_all_blocks()
        return False


def is_gpu_available():
    """Check if GPU acceleration is available"""
    return GPU_AVAILABLE


def get_array_module(arr):
    """Get the appropriate array module (cupy or numpy) for the given array"""
    if GPU_AVAILABLE and isinstance(arr, cp.ndarray):
        return cp
    return np


def to_gpu(arr, force_cpu=False):
    """
    Transfer array to GPU if available and beneficial.

    Args:
        arr: NumPy array to transfer
        force_cpu: If True, keep on CPU even if GPU is available

    Returns:
        GPU array if GPU available and not forced to CPU, otherwise original array
    """
    if force_cpu or not GPU_AVAILABLE:
        return arr

    try:
        # Only transfer to GPU if array is reasonably large
        # Small arrays may be faster on CPU due to transfer overhead
        if arr.nbytes > 1024 * 1024:  # 1 MB threshold
            return cp.asarray(arr)
    except Exception as e:
        print(f"Warning: GPU transfer failed, using CPU: {e}", file=sys.stderr)

    return arr


def to_cpu(arr):
    """
    Transfer array back to CPU (NumPy).

    Args:
        arr: Array (GPU or CPU) to transfer

    Returns:
        NumPy array
    """
    if GPU_AVAILABLE and isinstance(arr, cp.ndarray):
        return cp.asnumpy(arr)
    return arr


def gpu_accelerated_reshape_mean(data, new_shape, axis):
    """
    GPU-accelerated reshape and mean operation for downsampling.

    This is used in downsampling to perform 2x2 pixel averaging efficiently.

    Args:
        data: 2D array to downsample
        new_shape: Shape to reshape to (e.g., (512, 2, 512, 2))
        axis: Axis tuple to average over (e.g., (1, 3))

    Returns:
        Downsampled array (same type as input - GPU or CPU)
    """
    if not GPU_AVAILABLE:
        return data.reshape(new_shape).mean(axis=axis)

    try:
        # Transfer to GPU if not already there
        data_gpu = to_gpu(data)

        # Perform reshape and mean on GPU
        result_gpu = data_gpu.reshape(new_shape).mean(axis=axis)

        # Return in same format as input
        if isinstance(data, np.ndarray):
            return to_cpu(result_gpu)
        return result_gpu

    except Exception as e:
        print(f"Warning: GPU reshape+mean failed, using CPU: {e}", file=sys.stderr)
        return data.reshape(new_shape).mean(axis=axis)


def gpu_gaussian_filter(input_arr, sigma, truncate=4.0):
    """
    GPU-accelerated Gaussian filter with automatic fallback.

    Args:
        input_arr: Input array to filter
        sigma: Standard deviation for Gaussian kernel
        truncate: Truncate filter at this many standard deviations

    Returns:
        Filtered array (same type as input)
    """
    if not GPU_AVAILABLE:
        return cpu_ndimage.gaussian_filter(input_arr, sigma=sigma, truncate=truncate)

    try:
        # Transfer to GPU if not already there
        arr_gpu = to_gpu(input_arr)

        # Apply Gaussian filter on GPU
        result_gpu = gpu_ndimage.gaussian_filter(
            arr_gpu, sigma=sigma, truncate=truncate
        )

        # Return in same format as input
        if isinstance(input_arr, np.ndarray):
            return to_cpu(result_gpu)
        return result_gpu

    except Exception as e:
        print(f"Warning: GPU gaussian_filter failed, using CPU: {e}", file=sys.stderr)
        return cpu_ndimage.gaussian_filter(input_arr, sigma=sigma, truncate=truncate)


def gpu_binary_erosion(input_arr, iterations=1):
    """
    GPU-accelerated binary erosion with automatic fallback.

    Args:
        input_arr: Binary input array to erode
        iterations: Number of erosion iterations

    Returns:
        Eroded binary array (same type as input)
    """
    if not GPU_AVAILABLE:
        return cpu_ndimage.binary_erosion(input_arr, iterations=iterations)

    try:
        # Transfer to GPU if not already there
        arr_gpu = to_gpu(input_arr)

        # Apply binary erosion on GPU
        result_gpu = gpu_ndimage.binary_erosion(arr_gpu, iterations=iterations)

        # Return in same format as input
        if isinstance(input_arr, np.ndarray):
            return to_cpu(result_gpu)
        return result_gpu

    except Exception as e:
        print(f"Warning: GPU binary_erosion failed, using CPU: {e}", file=sys.stderr)
        return cpu_ndimage.binary_erosion(input_arr, iterations=iterations)


def gpu_array_operations(data, operations):
    """
    Execute a series of array operations on GPU if available.

    This is useful for operations like terrarium encoding where multiple
    array operations are chained together.

    Args:
        data: Input array
        operations: List of lambda functions that perform operations

    Returns:
        Result of all operations (same type as input)
    """
    if not GPU_AVAILABLE:
        result = data
        for op in operations:
            result = op(result)
        return result

    try:
        # Transfer to GPU
        data_gpu = to_gpu(data)

        # Execute operations on GPU
        result_gpu = data_gpu
        for op in operations:
            result_gpu = op(result_gpu)

        # Return in same format as input
        if isinstance(data, np.ndarray):
            return to_cpu(result_gpu)
        return result_gpu

    except Exception as e:
        print(f"Warning: GPU array operations failed, using CPU: {e}", file=sys.stderr)
        result = data
        for op in operations:
            result = op(result)
        return result


def optimize_terrarium_encoding(data):
    """
    GPU-accelerated terrarium encoding operations.

    This handles the math-heavy parts of converting elevation data
    to terrarium RGB format.

    Args:
        data: Elevation data array

    Returns:
        Tuple of (red, green, blue) channels as uint8 arrays
    """
    if not GPU_AVAILABLE:
        data_shifted = data + 32768.0
        red = (data_shifted // 256).astype(np.uint8)
        green = (data_shifted % 256).astype(np.uint8)
        blue = ((data_shifted - np.floor(data_shifted)) * 256).astype(np.uint8)
        return red, green, blue

    try:
        # Transfer to GPU
        data_gpu = to_gpu(data)

        data_shifted = data_gpu + 32768.0
        red_gpu = (data_shifted // 256).astype(cp.uint8)
        green_gpu = (data_shifted % 256).astype(cp.uint8)
        blue_gpu = ((data_shifted - cp.floor(data_shifted)) * 256).astype(cp.uint8)

        # Transfer back to CPU for final encoding
        return to_cpu(red_gpu), to_cpu(green_gpu), to_cpu(blue_gpu)

    except Exception as e:
        print(
            f"Warning: GPU terrarium encoding failed, using CPU: {e}", file=sys.stderr
        )
        data_shifted = data + 32768.0
        red = (data_shifted // 256).astype(np.uint8)
        green = (data_shifted % 256).astype(np.uint8)
        blue = ((data_shifted - np.floor(data_shifted)) * 256).astype(np.uint8)
        return red, green, blue


def print_gpu_info():
    """Print information about GPU availability and configuration"""
    if GPU_AVAILABLE:
        try:
            device = cp.cuda.Device()
            print(f"GPU Device: {device.name}")
            print(f"Compute Capability: {device.compute_capability}")
            print(f"Total Memory: {device.mem_info[1] / 1e9:.2f} GB")
            print(f"Free Memory: {device.mem_info[0] / 1e9:.2f} GB")
        except Exception as e:
            print(f"GPU available but could not get device info: {e}")
    else:
        print("No GPU available - all operations will use CPU")
