# Task v1.1.9

## Fix 1: 3 → 5 thẻ max
- range(3) → range(5)
- >= 3 → >= 5  
- log "3" → "5"
- "Đã thử 3 thẻ" → "Đã thử 5 thẻ"

## Fix 2: Proxy used_count khi dùng lần lượt (single profile)
- Profile có proxy_server gán sẵn từ pool (bulk-assign trước đó)
- Khi runner bắt đầu + profile.proxy_server != None → match proxy trong pool bằng host:port → tăng used_count
- Hàm: increment_proxy_used_by_server(proxy_server_str)
- Gọi trong run_task() sau khi xác định proxy

## Fix 3: Auto xoá hàng done/declined khỏi queue table
- Sau khi result được ghi → hàng có status done/declined/consumed/success KHÔNG hiển thị trong queue table
- Cách: loadQueue() lọc bỏ status done/failed/consumed/declined trước khi render (giữ pending+running)
- HOẶC: thêm nút xoá từng hàng (X) trên mỗi row
- Chọn: auto-ẩn status done/failed/consumed sau khi có result (UX sạch), nhưng giữ "declined" visible (user cần biết thẻ nào declined)
- Thực ra user nói "hàng đã xoá" = hàng có status done/consumed → ẩn tự động
- declined → vẫn show (user cần biết)

## Fix 4: Giữ declined_results.json sau update
- StartApp.bat: thêm backup/restore declined_results.json

## Status: IN PROGRESS
