#!/usr/bin/env python3
"""
GPU Acceleration Benchmark for Mapterhorn Pipeline

This script benchmarks GPU-accelerated operations against CPU versions
to validate correctness and measure performance improvements.

Usage:
    uv run python benchmark_gpu.py
"""

import time
import sys
import numpy as np
from scipy import ndimage as cpu_ndimage

import gpu_utils


def benchmark_operation(name, cpu_func, gpu_func, test_data, iterations=5):
    """
    Benchmark a single operation comparing CPU vs GPU performance.

    Args:
        name: Name of the operation being benchmarked
        cpu_func: Function to run on CPU
        gpu_func: Function to run on GPU
        test_data: Input data for the operation
        iterations: Number of iterations to average over

    Returns:
        Tuple of (cpu_time, gpu_time, speedup, results_match)
    """
    print(f"\n{'='*60}")
    print(f"Benchmarking: {name}")
    print(f"{'='*60}")
    print(f"Data shape: {test_data.shape}, dtype: {test_data.dtype}")
    print(f"Data size: {test_data.nbytes / 1024 / 1024:.2f} MB")

    # Warm-up runs
    _ = cpu_func(test_data)
    if gpu_utils.is_gpu_available():
        _ = gpu_func(test_data)

    # CPU benchmark
    cpu_times = []
    cpu_result = None
    for i in range(iterations):
        start = time.perf_counter()
        cpu_result = cpu_func(test_data)
        end = time.perf_counter()
        cpu_times.append(end - start)
        print(f"CPU iteration {i+1}/{iterations}: {cpu_times[-1]*1000:.2f} ms")

    cpu_time = np.median(cpu_times)

    # GPU benchmark
    gpu_times = []
    gpu_result = None
    if gpu_utils.is_gpu_available():
        for i in range(iterations):
            start = time.perf_counter()
            gpu_result = gpu_func(test_data)
            end = time.perf_counter()
            gpu_times.append(end - start)
            print(f"GPU iteration {i+1}/{iterations}: {gpu_times[-1]*1000:.2f} ms")

        gpu_time = np.median(gpu_times)
        speedup = cpu_time / gpu_time

        # Validate correctness
        # Use relaxed tolerance to account for GPU floating-point differences
        results_match = np.allclose(cpu_result, gpu_result, rtol=1e-3, atol=1e-4)

        print(f"\n{'─'*60}")
        print(f"CPU median time: {cpu_time*1000:.2f} ms")
        print(f"GPU median time: {gpu_time*1000:.2f} ms")
        print(f"Speedup: {speedup:.2f}x")
        print(f"Results match: {'✓ YES' if results_match else '✗ NO (ERROR!)'}")

        return cpu_time, gpu_time, speedup, results_match
    else:
        print(f"\nGPU not available - CPU only benchmark")
        print(f"CPU median time: {cpu_time*1000:.2f} ms")
        return cpu_time, None, None, None


def benchmark_downsampling():
    """Benchmark 2x2 pixel averaging for downsampling"""
    # Simulate full 1024x1024 tile data
    test_data = np.random.randn(1024, 1024).astype(np.float32) * 1000 + 500

    def cpu_version(data):
        return data.reshape((512, 2, 512, 2)).mean(axis=(1, 3))

    def gpu_version(data):
        return gpu_utils.gpu_accelerated_reshape_mean(data, (512, 2, 512, 2), (1, 3), force_gpu=True)

    return benchmark_operation(
        "Downsampling (2x2 pixel averaging)",
        cpu_version,
        gpu_version,
        test_data,
        iterations=10
    )


def benchmark_gaussian_filter():
    """Benchmark Gaussian filtering for tile blending"""
    # Simulate tile data with typical parameters
    test_data = np.random.randn(1024, 1024).astype(np.float32)
    sigma = 10
    truncate = 4

    def cpu_version(data):
        return cpu_ndimage.gaussian_filter(data, sigma=sigma, truncate=truncate)

    def gpu_version(data):
        return gpu_utils.gpu_gaussian_filter(data, sigma=sigma, truncate=truncate, force_gpu=True)

    return benchmark_operation(
        f"Gaussian Filter (sigma={sigma}, truncate={truncate})",
        cpu_version,
        gpu_version,
        test_data,
        iterations=5
    )


def benchmark_binary_erosion():
    """Benchmark binary erosion for mask operations"""
    # Simulate binary mask
    test_data = np.random.randint(0, 2, size=(1024, 1024), dtype=np.int32)

    def cpu_version(data):
        return cpu_ndimage.binary_erosion(data)

    def gpu_version(data):
        return gpu_utils.gpu_binary_erosion(data, force_gpu=True)

    return benchmark_operation(
        "Binary Erosion",
        cpu_version,
        gpu_version,
        test_data,
        iterations=10
    )


def benchmark_terrarium_encoding():
    """Benchmark terrarium RGB encoding"""
    # Simulate elevation data
    test_data = np.random.randn(512, 512).astype(np.float32) * 1000 + 500

    def cpu_version(data):
        data_shifted = data + 32768.0
        red = (data_shifted // 256).astype(np.uint8)
        green = np.floor(data_shifted % 256).astype(np.uint8)
        blue = (np.floor((data_shifted - np.floor(data_shifted)) * 256)).astype(np.uint8)
        return np.stack([red, green, blue], axis=-1)

    def gpu_version(data):
        red, green, blue = gpu_utils.optimize_terrarium_encoding(data, force_gpu=True)
        return np.stack([red, green, blue], axis=-1)

    return benchmark_operation(
        "Terrarium RGB Encoding",
        cpu_version,
        gpu_version,
        test_data,
        iterations=10
    )


def print_summary(results):
    """Print summary table of all benchmark results"""
    print(f"\n\n{'='*80}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*80}\n")

    if gpu_utils.is_gpu_available():
        gpu_utils.print_gpu_info()
        print()

        print(f"{'Operation':<40} {'CPU (ms)':<12} {'GPU (ms)':<12} {'Speedup':<10} {'Valid':<8}")
        print(f"{'-'*80}")

        for name, (cpu_time, gpu_time, speedup, valid) in results.items():
            if gpu_time is not None:
                valid_str = '✓' if valid else '✗ ERROR'
                print(f"{name:<40} {cpu_time*1000:>10.2f}   {gpu_time*1000:>10.2f}   {speedup:>8.2f}x  {valid_str}")
            else:
                print(f"{name:<40} {cpu_time*1000:>10.2f}   {'N/A':<10}   {'N/A':<8}  {'N/A'}")

        print(f"{'-'*80}")

        # Calculate overall metrics
        speedups = [v[2] for v in results.values() if v[2] is not None]
        if speedups:
            avg_speedup = np.mean(speedups)
            print(f"\nAverage GPU Speedup: {avg_speedup:.2f}x")

        all_valid = all(v[3] for v in results.values() if v[3] is not None)
        if all_valid:
            print("✓ All GPU results match CPU results - correctness validated!")
        else:
            print("✗ WARNING: Some GPU results do not match CPU - check implementation!")

    else:
        print("GPU not available - only CPU benchmarks were run")
        print(f"\n{'Operation':<40} {'CPU (ms)':<12}")
        print(f"{'-'*52}")
        for name, (cpu_time, _, _, _) in results.items():
            print(f"{name:<40} {cpu_time*1000:>10.2f}")

    print()


def main():
    """Run all benchmarks and display results"""
    print("\n" + "="*80)
    print("Mapterhorn GPU Acceleration Benchmark")
    print("="*80)

    if not gpu_utils.is_gpu_available():
        print("\n⚠️  GPU acceleration not available")
        print("To enable GPU acceleration:")
        print("  1. Ensure you have an NVIDIA GPU with CUDA support")
        print("  2. Install CUDA toolkit (12.x recommended)")
        print("  3. Install GPU dependencies: uv pip install -e '.[gpu]'")
        print("\nRunning CPU-only benchmarks...\n")
    else:
        print("\n✓ GPU acceleration is available")
        print("\nRunning benchmarks...\n")

    results = {}

    try:
        results['Downsampling (2x2 averaging)'] = benchmark_downsampling()
    except Exception as e:
        print(f"ERROR in downsampling benchmark: {e}")
        results['Downsampling (2x2 averaging)'] = (0, None, None, False)

    try:
        results['Gaussian Filter'] = benchmark_gaussian_filter()
    except Exception as e:
        print(f"ERROR in Gaussian filter benchmark: {e}")
        results['Gaussian Filter'] = (0, None, None, False)

    try:
        results['Binary Erosion'] = benchmark_binary_erosion()
    except Exception as e:
        print(f"ERROR in binary erosion benchmark: {e}")
        results['Binary Erosion'] = (0, None, None, False)

    try:
        results['Terrarium Encoding'] = benchmark_terrarium_encoding()
    except Exception as e:
        print(f"ERROR in terrarium encoding benchmark: {e}")
        results['Terrarium Encoding'] = (0, None, None, False)

    print_summary(results)

    # Exit with error code if GPU was available but validation failed
    if gpu_utils.is_gpu_available():
        all_valid = all(v[3] for v in results.values() if v[3] is not None)
        if not all_valid:
            sys.exit(1)


if __name__ == '__main__':
    main()
