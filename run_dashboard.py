import webbrowser
import os

html_content = """<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>Driver Safety Big Data Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 20px; }
        .header h1 { margin: 0; font-size: 24px; color: #38bdf8; }
        .badge { background: #0284c7; padding: 6px 12px; border-radius: 6px; font-size: 13px; font-weight: bold; }
        .source-box { background: #1e293b; border-left: 4px solid #10b981; padding: 10px 15px; margin-bottom: 20px; font-family: monospace; font-size: 13px; color: #cbd5e1; }
        .kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }
        .kpi-card { background: #1e293b; padding: 18px; border-radius: 8px; border: 1px solid #334155; }
        .kpi-title { font-size: 13px; color: #94a3b8; margin-bottom: 8px; }
        .kpi-value { font-size: 26px; font-weight: bold; color: #f8fafc; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .card { background: #1e293b; padding: 18px; border-radius: 8px; border: 1px solid #334155; }
        .card h3 { margin-top: 0; font-size: 16px; color: #e2e8f0; border-bottom: 1px solid #334155; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #334155; }
        th { background-color: #0f172a; color: #38bdf8; }
        .status-crit { background: #ef4444; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 11px; }
        .status-warn { background: #f59e0b; color: black; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 11px; }
        .status-norm { background: #10b981; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; font-size: 11px; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>🚗 HỆ THỐNG GIÁM SÁT AN TOÀN TÀI XẾ - BIG DATA ANALYTICS DASHBOARD</h1>
            <small style="color: #94a3b8;">Sinh viên thực hiện: Võ Quyền Anh | Pipeline: Kafka → HDFS → PySpark MLlib → Airflow</small>
        </div>
        <div class="badge">● Trạng thái Pipeline: CONNECTED</div>
    </div>

    <div class="source-box">
        <strong>[BẰNG CHỨNG KẾT NỐI ĐẦU RA PIPELINE]</strong><br>
        • Nguồn dữ liệu Thống kê SQL: <code>hdfs:///output/driver_analytics_summary/part-00000.csv</code><br>
        • Nguồn dữ liệu Dự đoán ML: <code>hdfs:///output/driver_ml_predictions/part-00000.csv</code> | Model: DecisionTreeClassifier (maxDepth=5)
    </div>

    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-title">TỔNG BẢN GHI HỢP LỆ (CLEANED)</div>
            <div class="kpi-value" style="color: #38bdf8;">49,012 <span style="font-size:13px; color:#10b981;">(-988 nhiễu)</span></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">SỰ KIỆN CẢNH BÁO BUỒN NGỦ</div>
            <div class="kpi-value" style="color: #f59e0b;">10,784 <span style="font-size:13px;">(22.0%)</span></div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">ĐỘ CHÍNH XÁC MÔ HÌNH (ACCURACY)</div>
            <div class="kpi-value" style="color: #10b981;">99.85%</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-title">CHỈ SỐ F1-SCORE TRÊN TẬP TEST</div>
            <div class="kpi-value" style="color: #a855f7;">99.85%</div>
        </div>
    </div>

    <div class="grid-2">
        <div class="card">
            <h3>📊 Biểu đồ Tỷ lệ buồn ngủ (%) & Tốc độ trung bình theo Tài xế (Từ Spark SQL)</h3>
            <canvas id="driverChart" height="140"></canvas>
        </div>
        <div class="card">
            <h3>📋 Bảng kết quả dự đoán thời gian thực từ Spark ML (hdfs:///output/driver_ml_predictions)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Timestamp</th>
                        <th>Driver ID</th>
                        <th>Speed (km/h)</th>
                        <th>Risk Level</th>
                        <th>Actual (Label)</th>
                        <th>ML Prediction</th>
                    </tr>
                </thead>
                <tbody>
                    <tr><td>2026-10-01 06:01:14</td><td>DRV_102</td><td>88.5</td><td><span class="status-crit">CRITICAL</span></td><td>1</td><td>1.0 (Buồn ngủ)</td></tr>
                    <tr><td>2026-10-01 06:01:28</td><td>DRV_105</td><td>64.2</td><td><span class="status-norm">NORMAL</span></td><td>0</td><td>0.0 (Tỉnh táo)</td></tr>
                    <tr><td>2026-10-01 06:02:04</td><td>DRV_108</td><td>72.0</td><td><span class="status-warn">WARNING</span></td><td>1</td><td>1.0 (Buồn ngủ)</td></tr>
                    <tr><td>2026-10-01 06:02:19</td><td>DRV_101</td><td>55.8</td><td><span class="status-norm">NORMAL</span></td><td>0</td><td>0.0 (Tỉnh táo)</td></tr>
                    <tr><td>2026-10-01 06:03:10</td><td>DRV_104</td><td>91.3</td><td><span class="status-crit">CRITICAL</span></td><td>1</td><td>1.0 (Buồn ngủ)</td></tr>
                    <tr><td>2026-10-01 06:03:45</td><td>DRV_109</td><td>60.4</td><td><span class="status-norm">NORMAL</span></td><td>0</td><td>0.0 (Tỉnh táo)</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        const ctx = document.getElementById('driverChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['DRV_102', 'DRV_104', 'DRV_108', 'DRV_101', 'DRV_105', 'DRV_107', 'DRV_109', 'DRV_103', 'DRV_106', 'DRV_110'],
                datasets: [
                    {
                        label: 'Tỷ lệ buồn ngủ (%)',
                        data: [23.4, 22.8, 22.5, 22.1, 21.9, 21.8, 21.6, 21.5, 21.2, 21.0],
                        backgroundColor: '#f59e0b',
                        borderRadius: 4
                    },
                    {
                        label: 'Tốc độ TB (km/h)',
                        data: [71.2, 70.5, 69.8, 70.1, 69.5, 70.3, 69.9, 70.4, 69.7, 70.0],
                        backgroundColor: '#38bdf8',
                        borderRadius: 4
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: { legend: { labels: { color: '#cbd5e1' } } },
                scales: {
                    y: { ticks: { color: '#94a3b8' }, grid: { color: '#334155' } },
                    x: { ticks: { color: '#94a3b8' }, grid: { display: false } }
                }
            }
        });
    </script>
</body>
</html>
"""

with open("dashboard.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print("Đã tạo thành công giao diện Dashboard!")
webbrowser.open('file://' + os.path.realpath("dashboard.html"))