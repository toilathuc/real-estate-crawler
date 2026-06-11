Mục đích: giải thích “tmp” trong dự án.

Trong dự án này, các lệnh chạy benchmark/perf đang ghi output vào thư mục hệ thống `/tmp` trên máy chạy (Linux), ví dụ:
- /tmp/perf-report-smoke/out/vm_comparison.html
- /tmp/spark-events/
- /tmp/*.log

Vì vậy sẽ không có thư mục `tmp` trong repo để bạn tự copy “tmp” vào.

Nếu muốn đưa output vào repo, nên tạo cấu trúc dữ liệu nhỏ (ví dụ chỉ commit các file report html/json cần thiết) thay vì commit toàn bộ spark event logs (thường quá lớn).

