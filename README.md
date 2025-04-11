# Excel File Splitter

Công cụ này giúp chia một file Excel lớn thành nhiều file Excel nhỏ hơn, phù hợp khi cần xử lý file Excel có kích thước lớn (100MB+).

## Tính năng

- Chia file Excel lớn thành nhiều file nhỏ hơn
- Xác định số lượng sheet trong mỗi file đầu ra
- Xác định số lượng hàng tối đa trong mỗi sheet
- Xử lý theo từng phần (chunk) để tiết kiệm bộ nhớ
- Theo dõi quá trình xử lý thông qua logging
- Tối ưu bộ nhớ khi xử lý file lớn
- Hỗ trợ xử lý song song trên nhiều CPU để tăng tốc

## Yêu cầu

```
pip install pandas openpyxl numpy psutil matplotlib
```

## Cách sử dụng

### Cơ bản

```bash
python excel_splitter.py input.xlsx output_folder
```

### Tùy chọn

```bash
python excel_splitter.py input.xlsx output_folder --sheets 3 --rows 40000 --chunk-size 10000
```

Trong đó:
- `input.xlsx`: File Excel đầu vào cần chia
- `output_folder`: Thư mục lưu các file đầu ra
- `--sheets`: Số lượng sheet trong mỗi file đầu ra (mặc định: 3)
- `--rows`: Số hàng dữ liệu tối đa trong mỗi sheet (mặc định: 40000)
- `--chunk-size`: Số hàng đọc mỗi lần để tiết kiệm bộ nhớ (mặc định: 10000)

## Các phiên bản

Dự án này cung cấp 3 phiên bản khác nhau để đáp ứng các nhu cầu khác nhau:

### 1. Phiên bản cơ bản (split_excel.py)

Phiên bản đơn giản, phù hợp cho các file Excel có kích thước vừa phải (<100MB):

```bash
python split_excel.py input.xlsx output_folder --sheets 3 --rows 40000
```

### 2. Phiên bản tối ưu bộ nhớ (excel_splitter.py)

Phiên bản được tối ưu hóa để sử dụng ít bộ nhớ hơn, phù hợp cho file Excel lớn (100MB-500MB):

```bash
python excel_splitter.py input.xlsx output_folder --chunk-size 5000
```

### 3. Phiên bản xử lý song song (excel_splitter_parallel.py)

Phiên bản mạnh mẽ nhất, sử dụng đa luồng để xử lý các sheet cùng lúc trên nhiều CPU:

```bash
python excel_splitter_parallel.py input.xlsx output_folder --workers 4
```

Tham số:
- `--workers`: Số lượng worker (tiến trình con) chạy song song (mặc định: số CPU-1)

Phiên bản này đặc biệt hữu ích cho:
- File Excel rất lớn (>500MB)
- Hệ thống có nhiều CPU/lõi
- Cần xử lý nhanh chóng
- File có nhiều sheet

## Benchmark

Công cụ này có kèm theo một script benchmark để so sánh hiệu suất của các phương pháp xử lý. Bạn có thể chạy benchmark như sau:

```bash
python benchmark.py --generate --input-file test.xlsx --output-dir benchmark_results
```

Các tùy chọn:
- `--generate`: Tạo file Excel test trước khi chạy benchmark
- `--input-file`: Đường dẫn file Excel đầu vào (mặc định: test_file.xlsx)
- `--output-dir`: Thư mục lưu kết quả benchmark (mặc định: benchmark_results)
- `--sheets`: Số lượng sheet trong mỗi file đầu ra (mặc định: 3)
- `--rows`: Số hàng tối đa trong mỗi sheet đầu ra (mặc định: 40000)
- `--chunk-size`: Kích thước chunk khi xử lý (mặc định: 10000)
- `--test-sheets`: Số lượng sheet trong file test (mặc định: 5)
- `--test-rows`: Số hàng trong mỗi sheet của file test (mặc định: 50000)

Kết quả benchmark sẽ được hiển thị trên terminal và lưu dưới dạng biểu đồ trong file `benchmark_results/benchmark_results.png`.

## Hiệu suất thực tế

Dưới đây là kết quả hiệu suất dự kiến cho mỗi phương pháp khi xử lý file Excel 100MB (5 sheets, mỗi sheet 50.000 hàng):

| Phương pháp | Thời gian | Bộ nhớ sử dụng | Khi nào nên dùng |
|-------------|-----------|----------------|------------------|
| Cơ bản | ~ 60 giây | ~ 500MB | File nhỏ, máy có RAM cao |
| Tối ưu | ~ 90 giây | ~ 200MB | File lớn, máy có RAM thấp |
| Song song | ~ 30 giây | ~ 300MB | File lớn, máy nhiều CPU |

## Ghi chú

- Công cụ này hiệu quả nhất khi file đầu vào có cấu trúc đơn giản (chỉ có dữ liệu dạng bảng).
- Với những file Excel rất lớn (>500MB), bạn có thể cần điều chỉnh giảm `chunk_size` để tránh lỗi hết bộ nhớ.
- Quá trình xử lý được ghi lại trong file log tương ứng với mỗi script.
- Phiên bản xử lý song song có thể gặp xung đột khi nhiều tiến trình cùng truy cập vào file đầu ra, hãy kiểm tra kỹ kết quả khi sử dụng phiên bản này. 



BEST CMD:
python excel_splitter_db.py input/11042025.xlsx output/ --sheets 3 --rows 40000 --known-sheets 80 --known-rows 50001 --workers 6 --db /home/ethan/work/ai/tool_chia_excel/db/excel_data.db --delete-db 0