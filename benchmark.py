#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
import pandas as pd
import numpy as np
import psutil
import matplotlib.pyplot as plt
import gc
from split_excel import split_excel_file
from excel_splitter import split_excel_file_with_optimized_memory

def generate_test_file(output_file, num_sheets=5, rows_per_sheet=50000, cols=2):
    """
    Tạo file Excel test với số lượng sheet và dữ liệu theo yêu cầu
    """
    print(f"Đang tạo file test: {output_file}")
    print(f"- Số sheets: {num_sheets}")
    print(f"- Số hàng mỗi sheet: {rows_per_sheet}")
    print(f"- Số cột: {cols}")
    
    writer = pd.ExcelWriter(output_file, engine='openpyxl')
    
    for i in range(num_sheets):
        # Tạo dữ liệu ngẫu nhiên
        data = {}
        for j in range(cols):
            data[f'Column_{j+1}'] = np.random.randint(0, 1000000, size=rows_per_sheet)
        
        df = pd.DataFrame(data)
        
        # Ghi vào file
        print(f"Đang ghi sheet {i+1}/{num_sheets}...")
        df.to_excel(writer, sheet_name=f'Sheet_{i+1}', index=False)
    
    writer.close()
    print(f"Đã tạo xong file test: {output_file}")
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"Kích thước file: {file_size_mb:.2f} MB")
    return output_file

def run_benchmark(input_file, output_dir, sheets_per_file=3, rows_per_sheet=40000, chunk_size=10000):
    """
    Chạy benchmark và so sánh hiệu suất của 2 phương pháp
    """
    # Tạo thư mục đầu ra nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Tạo thư mục cho từng phương pháp
    output_dir_basic = os.path.join(output_dir, "basic")
    output_dir_optimized = os.path.join(output_dir, "optimized")
    os.makedirs(output_dir_basic, exist_ok=True)
    os.makedirs(output_dir_optimized, exist_ok=True)
    
    # Xóa các file cũ trong thư mục đầu ra
    for folder in [output_dir_basic, output_dir_optimized]:
        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            if os.path.isfile(file_path) and file.endswith('.xlsx'):
                os.remove(file_path)
    
    # Đo hiệu suất phương pháp cơ bản
    print("\n===== BENCHMARK PHƯƠNG PHÁP CƠ BẢN =====")
    start_time = time.time()
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss / (1024 * 1024)
    
    try:
        split_excel_file(input_file, output_dir_basic, sheets_per_file, rows_per_sheet)
        basic_success = True
    except Exception as e:
        print(f"Lỗi khi chạy phương pháp cơ bản: {e}")
        basic_success = False
    
    end_memory = process.memory_info().rss / (1024 * 1024)
    end_time = time.time()
    basic_time = end_time - start_time
    basic_memory = end_memory - start_memory
    
    # Thu hồi bộ nhớ
    gc.collect()
    
    # Đo hiệu suất phương pháp tối ưu
    print("\n===== BENCHMARK PHƯƠNG PHÁP TỐI ƯU =====")
    start_time = time.time()
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss / (1024 * 1024)
    
    try:
        split_excel_file_with_optimized_memory(
            input_file, output_dir_optimized, sheets_per_file, rows_per_sheet, chunk_size
        )
        optimized_success = True
    except Exception as e:
        print(f"Lỗi khi chạy phương pháp tối ưu: {e}")
        optimized_success = False
    
    end_memory = process.memory_info().rss / (1024 * 1024)
    end_time = time.time()
    optimized_time = end_time - start_time
    optimized_memory = end_memory - start_memory
    
    # Tạo báo cáo
    print("\n===== KẾT QUẢ BENCHMARK =====")
    print(f"File đầu vào: {input_file}")
    print(f"Kích thước file: {os.path.getsize(input_file) / (1024 * 1024):.2f} MB")
    
    if basic_success:
        print(f"\nPhương pháp cơ bản:")
        print(f"- Thời gian xử lý: {basic_time:.2f} giây")
        print(f"- Bộ nhớ sử dụng (tăng thêm): {basic_memory:.2f} MB")
        basic_files = [f for f in os.listdir(output_dir_basic) if f.endswith('.xlsx')]
        print(f"- Số file đầu ra: {len(basic_files)}")
    else:
        print("Phương pháp cơ bản: THẤT BẠI")
    
    if optimized_success:
        print(f"\nPhương pháp tối ưu:")
        print(f"- Thời gian xử lý: {optimized_time:.2f} giây")
        print(f"- Bộ nhớ sử dụng (tăng thêm): {optimized_memory:.2f} MB")
        optimized_files = [f for f in os.listdir(output_dir_optimized) if f.endswith('.xlsx')]
        print(f"- Số file đầu ra: {len(optimized_files)}")
    else:
        print("Phương pháp tối ưu: THẤT BẠI")
    
    if basic_success and optimized_success:
        time_improvement = (basic_time - optimized_time) / basic_time * 100
        memory_improvement = (basic_memory - optimized_memory) / basic_memory * 100
        
        print(f"\nSo sánh:")
        print(f"- Thời gian: phương pháp tối ưu {'nhanh' if time_improvement > 0 else 'chậm'} hơn {abs(time_improvement):.2f}%")
        print(f"- Bộ nhớ: phương pháp tối ưu {'tiết kiệm' if memory_improvement > 0 else 'tốn'} hơn {abs(memory_improvement):.2f}%")
        
        # Vẽ biểu đồ so sánh
        plot_comparison(basic_time, optimized_time, basic_memory, optimized_memory, 
                        os.path.join(output_dir, "benchmark_results.png"))
    
    return basic_success, optimized_success

def plot_comparison(basic_time, optimized_time, basic_memory, optimized_memory, output_file):
    """
    Vẽ biểu đồ so sánh hiệu suất
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Biểu đồ thời gian
    methods = ['Cơ bản', 'Tối ưu']
    times = [basic_time, optimized_time]
    ax1.bar(methods, times, color=['#3498db', '#2ecc71'])
    ax1.set_ylabel('Thời gian (giây)')
    ax1.set_title('Thời gian xử lý')
    
    # Thêm giá trị
    for i, v in enumerate(times):
        ax1.text(i, v + 0.1, f"{v:.2f}s", ha='center')
    
    # Biểu đồ bộ nhớ
    memories = [basic_memory, optimized_memory]
    ax2.bar(methods, memories, color=['#3498db', '#2ecc71'])
    ax2.set_ylabel('Bộ nhớ (MB)')
    ax2.set_title('Bộ nhớ sử dụng')
    
    # Thêm giá trị
    for i, v in enumerate(memories):
        ax2.text(i, v + 0.1, f"{v:.2f}MB", ha='center')
    
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Đã lưu biểu đồ so sánh: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Benchmark công cụ chia file Excel')
    parser.add_argument('--generate', action='store_true', help='Tạo file test trước khi benchmark')
    parser.add_argument('--input-file', default='test_file.xlsx', help='File Excel đầu vào để benchmark')
    parser.add_argument('--output-dir', default='benchmark_results', help='Thư mục lưu kết quả')
    parser.add_argument('--sheets', type=int, default=3, help='Số lượng sheet trong mỗi file đầu ra')
    parser.add_argument('--rows', type=int, default=40000, help='Số hàng dữ liệu tối đa trong mỗi sheet')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Số hàng đọc mỗi lần')
    parser.add_argument('--test-sheets', type=int, default=5, help='Số sheets trong file test')
    parser.add_argument('--test-rows', type=int, default=50000, help='Số hàng mỗi sheet trong file test')
    
    args = parser.parse_args()
    
    if args.generate:
        generate_test_file(args.input_file, args.test_sheets, args.test_rows)
    
    if not os.path.isfile(args.input_file):
        print(f"Lỗi: File đầu vào không tồn tại: {args.input_file}")
        return
    
    run_benchmark(args.input_file, args.output_dir, args.sheets, args.rows, args.chunk_size)

if __name__ == "__main__":
    main() 