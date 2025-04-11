#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import math
import argparse
from pathlib import Path
import time

def split_excel_file(input_file, output_dir, sheets_per_file=3, rows_per_sheet=40000):
    """
    Chia file Excel lớn thành nhiều file nhỏ
    
    Tham số:
        input_file (str): Đường dẫn đến file Excel đầu vào
        output_dir (str): Thư mục lưu các file đầu ra
        sheets_per_file (int): Số lượng sheet trong mỗi file đầu ra
        rows_per_sheet (int): Số hàng dữ liệu tối đa trong mỗi sheet
    """
    start_time = time.time()
    print(f"Bắt đầu xử lý file: {input_file}")
    
    # Tạo thư mục đầu ra nếu chưa tồn tại
    os.makedirs(output_dir, exist_ok=True)
    
    # Đọc tất cả sheets từ file đầu vào
    print("Đọc danh sách sheets từ file đầu vào...")
    xl = pd.ExcelFile(input_file)
    sheet_names = xl.sheet_names
    
    print(f"Tìm thấy {len(sheet_names)} sheets trong file đầu vào")
    
    # Đọc dữ liệu từ tất cả các sheets
    all_data = []
    for sheet_name in sheet_names:
        print(f"Đọc dữ liệu từ sheet: {sheet_name}")
        df = xl.parse(sheet_name)
        all_data.append((sheet_name, df))
    
    # Tính toán số lượng sheet đầu ra cần tạo
    total_rows = sum(len(df) for _, df in all_data)
    total_output_sheets = math.ceil(total_rows / rows_per_sheet)
    total_output_files = math.ceil(total_output_sheets / sheets_per_file)
    
    print(f"Tổng số hàng: {total_rows}")
    print(f"Số sheets đầu ra: {total_output_sheets}")
    print(f"Số files đầu ra: {total_output_files}")
    
    # Chia dữ liệu ra các file đầu ra
    current_file_index = 0
    current_sheet_index = 0
    remaining_rows = rows_per_sheet
    writer = None
    
    for original_sheet_name, df in all_data:
        rows_left = len(df)
        row_start = 0
        
        while rows_left > 0:
            # Nếu cần tạo file mới
            if current_sheet_index % sheets_per_file == 0 and remaining_rows == rows_per_sheet:
                if writer is not None:
                    print(f"Lưu file: {output_path}")
                    writer.close()
                
                output_filename = f"output_{current_file_index + 1}.xlsx"
                output_path = os.path.join(output_dir, output_filename)
                print(f"Tạo file mới: {output_path}")
                writer = pd.ExcelWriter(output_path, engine='openpyxl')
                current_file_index += 1
            
            # Tính toán số hàng cần lấy
            rows_to_take = min(rows_left, remaining_rows)
            
            # Tên sheet mới
            new_sheet_name = f"Sheet_{current_sheet_index % sheets_per_file + 1}"
            
            # Dữ liệu cho sheet mới
            if (current_sheet_index % sheets_per_file) == sheets_per_file - 1 and current_file_index == total_output_files:
                # Sheet cuối cùng của file cuối cùng - có thể chứa số hàng lẻ
                print(f"Thêm {rows_to_take} hàng vào {new_sheet_name} (sheet cuối của file cuối)")
                if remaining_rows == rows_per_sheet:
                    # Sheet mới hoàn toàn
                    chunk = df.iloc[row_start:row_start + rows_to_take]
                else:
                    # Thêm vào sheet đã tồn tại
                    if os.path.exists(output_path) and new_sheet_name in pd.ExcelFile(output_path).sheet_names:
                        existing_df = pd.read_excel(output_path, sheet_name=new_sheet_name)
                        chunk = pd.concat([existing_df, df.iloc[row_start:row_start + rows_to_take]])
                    else:
                        chunk = df.iloc[row_start:row_start + rows_to_take]
                
                chunk.to_excel(writer, sheet_name=new_sheet_name, index=False)
            else:
                # Các sheet thông thường
                print(f"Thêm {rows_to_take} hàng vào {new_sheet_name}")
                if remaining_rows == rows_per_sheet:
                    # Sheet mới hoàn toàn
                    chunk = df.iloc[row_start:row_start + rows_to_take]
                    chunk.to_excel(writer, sheet_name=new_sheet_name, index=False)
                else:
                    # Thêm vào sheet đã tồn tại
                    if new_sheet_name in writer.sheets:
                        existing_df = pd.read_excel(writer.path, sheet_name=new_sheet_name)
                        chunk = pd.concat([existing_df, df.iloc[row_start:row_start + rows_to_take]])
                        chunk.to_excel(writer, sheet_name=new_sheet_name, index=False)
                    else:
                        chunk = df.iloc[row_start:row_start + rows_to_take]
                        chunk.to_excel(writer, sheet_name=new_sheet_name, index=False)
            
            # Cập nhật các biến đếm
            row_start += rows_to_take
            rows_left -= rows_to_take
            remaining_rows -= rows_to_take
            
            # Nếu sheet đã đầy, chuyển sang sheet mới
            if remaining_rows == 0:
                current_sheet_index += 1
                remaining_rows = rows_per_sheet
    
    # Đóng file Excel cuối cùng
    if writer is not None:
        writer.close()
    
    end_time = time.time()
    print(f"Hoàn thành trong {end_time - start_time:.2f} giây")

def main():
    parser = argparse.ArgumentParser(description='Chia file Excel lớn thành nhiều file nhỏ hơn')
    parser.add_argument('input_file', help='Đường dẫn đến file Excel đầu vào')
    parser.add_argument('output_dir', help='Thư mục lưu các file đầu ra')
    parser.add_argument('--sheets', type=int, default=3, help='Số lượng sheet trong mỗi file đầu ra (mặc định: 3)')
    parser.add_argument('--rows', type=int, default=40000, help='Số hàng dữ liệu tối đa trong mỗi sheet (mặc định: 40000)')
    
    args = parser.parse_args()
    
    # Kiểm tra file đầu vào tồn tại
    if not os.path.isfile(args.input_file):
        print(f"Lỗi: File đầu vào không tồn tại: {args.input_file}")
        return
    
    split_excel_file(args.input_file, args.output_dir, args.sheets, args.rows)

if __name__ == "__main__":
    main() 