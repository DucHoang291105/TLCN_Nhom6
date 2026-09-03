from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import os

BASE_DIR=r"D:\Code\Năm 4\TLCN"
OUTPUT_PATH=os.path.join(BASE_DIR,"docs","data_dictionary.xlsx")

columns=[
    ["name","title","string","string","Tiêu đề tin đăng","No","Trim khoảng trắng"],
    ["description","description","string","string","Nội dung mô tả bất động sản","No","Giữ nguyên, trim"],
    ["property_type_name","property_type","string","string","Loại hình bất động sản","No","Chuẩn hóa category"],
    ["province_name","province_name","string","string","Tỉnh/thành phố","No","Chuẩn hóa tên địa phương"],
    ["district_name","district_name","string","string","Quận/huyện","Yes","Trim, cho phép null"],
    ["ward_name","ward_name","string","string","Phường/xã","Yes","Trim, cho phép null"],
    ["street_name","street_name","string","string","Tên đường","Yes","Trim"],
    ["project_name","project_name","string","string","Tên dự án","Yes","Trim"],
    ["price","price","string","decimal(20,0)","Giá bất động sản, đơn vị VND","Yes","Cast string sang numeric"],
    ["area","area_m2","double","double","Diện tích bất động sản, đơn vị m²","Yes","Validate và kiểm tra outlier"],
    ["floor_count","floor_count","double","integer","Số tầng","Yes","Validate và cast integer"],
    ["frontage_width","frontage_width_m","double","double","Chiều rộng mặt tiền, mét","Yes","Validate"],
    ["house_depth","house_depth_m","double","double","Chiều sâu bất động sản, mét","Yes","Validate"],
    ["road_width","road_width_m","double","double","Chiều rộng đường, mét","Yes","Validate"],
    ["bedroom_count","bedroom_count","double","integer","Số phòng ngủ","Yes","Validate và cast integer"],
    ["bathroom_count","bathroom_count","double","integer","Số phòng tắm","Yes","Validate và cast integer"],
    ["house_direction","house_direction","string","string","Hướng nhà","Yes","Chuẩn hóa category"],
    ["balcony_direction","balcony_direction","string","string","Hướng ban công","Yes","Chuẩn hóa category"],
    ["published_at","published_at","string","timestamp","Thời gian đăng tin","No","Cast string sang timestamp"],

    ["-","listing_id","-","string","ID nội bộ của bản ghi","No","Sinh ID từ dữ liệu nguồn"],
    ["-","price_per_m2","-","double","Giá trên mỗi mét vuông","Yes","price / area_m2"],
    ["-","published_date","-","date","Ngày đăng tin","No","Tách từ published_at"],
    ["-","year","-","integer","Năm đăng tin","No","Tách từ published_at"],
    ["-","month","-","integer","Tháng đăng tin","No","Tách từ published_at"],
    ["-","year_month","-","string","Tháng phân tích dạng YYYY-MM","No","Tạo từ published_at"],
    ["-","dq_status","-","string","Trạng thái chất lượng dữ liệu","No","VALID / REVIEW / REJECTED"],
    ["-","dq_error_codes","-","string","Danh sách mã lỗi chất lượng dữ liệu","Yes","Sinh từ Data Quality Rules"]
]

headers=[
    "raw_column",
    "silver_column",
    "raw_type",
    "silver_type",
    "description",
    "nullable",
    "transformation"
]

wb=Workbook()
ws=wb.active
ws.title="data_dictionary"

ws.append(headers)

for row in columns:
    ws.append(row)

# Header
header_fill=PatternFill("solid",fgColor="1F4E78")
header_font=Font(color="FFFFFF",bold=True)

for cell in ws[1]:
    cell.fill=header_fill
    cell.font=header_font
    cell.alignment=Alignment(horizontal="center",vertical="center")

# Border
thin=Side(style="thin",color="B7B7B7")

for row in ws.iter_rows():
    for cell in row:
        cell.border=Border(
            left=thin,
            right=thin,
            top=thin,
            bottom=thin
        )
        cell.alignment=Alignment(
            vertical="top",
            wrap_text=True
        )

# Width
widths={
    "A":24,
    "B":24,
    "C":15,
    "D":18,
    "E":40,
    "F":12,
    "G":40
}

for col,width in widths.items():
    ws.column_dimensions[col].width=width

ws.row_dimensions[1].height=25
ws.freeze_panes="A2"
ws.auto_filter.ref=ws.dimensions

os.makedirs(os.path.dirname(OUTPUT_PATH),exist_ok=True)
wb.save(OUTPUT_PATH)

print("DATA DICTIONARY CREATED")
print("Output:",OUTPUT_PATH)
print("Rows:",len(columns))