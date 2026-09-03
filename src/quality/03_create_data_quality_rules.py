from openpyxl import Workbook
from openpyxl.styles import Font,PatternFill,Alignment,Border,Side
import os

BASE_DIR=r"D:\Code\Năm 4\TLCN"
OUTPUT_PATH=os.path.join(BASE_DIR,"docs","data_quality_rules.xlsx")

rules=[
    ["DQ01","price","price IS NULL","Giá bán bị thiếu","REVIEW","Không dùng record này khi tính KPI liên quan đến giá"],
    ["DQ02","price","price = 0","Giá bán bằng 0","REJECTED","Loại khỏi các bảng Gold phân tích giá"],
    ["DQ03","price","TRY_CAST(price AS numeric) IS NULL","Giá không chuyển được sang số","REJECTED","Loại khỏi Silver analytics"],
    ["DQ04","area","area IS NULL OR area <= 0","Diện tích thiếu hoặc không hợp lệ","REJECTED","Không thể tính price_per_m2"],
    ["DQ05","area","area > P99 theo property_type","Diện tích bất thường so với cùng loại BĐS","REVIEW","Giữ record nhưng đánh dấu để kiểm tra"],
    ["DQ06","price_per_m2","price_per_m2 > P99 theo property_type","Giá/m² bất thường so với cùng loại BĐS","REVIEW","Giữ record nhưng không dùng mặc định cho KPI sạch"],
    ["DQ07","province_name","province_name IS NULL OR TRIM(province_name)=''","Thiếu tỉnh/thành phố","REJECTED","Không thể phân tích theo vị trí"],
    ["DQ08","district_name","district_name IS NULL","Thiếu quận/huyện","REVIEW","Vẫn giữ được phân tích cấp tỉnh"],
    ["DQ09","ward_name","ward_name IS NULL","Thiếu phường/xã","REVIEW","Vẫn giữ được phân tích cấp tỉnh/quận"],
    ["DQ10","published_at","Không parse được timestamp","Ngày đăng không hợp lệ","REJECTED","Không thể phân tích theo thời gian"],
    ["DQ11","record","Trùng khóa nội dung xác định","Bản ghi trùng lặp","REJECTED","Giữ một bản ghi, loại bản sao"],
    ["DQ12","floor_count","Giá trị cực đoan/bất thường","Số tầng bất thường","REVIEW","Kiểm tra theo phân phối dữ liệu"],
    ["DQ13","bedroom_count","Giá trị cực đoan/bất thường","Số phòng ngủ bất thường","REVIEW","Kiểm tra theo phân phối dữ liệu"],
    ["DQ14","bathroom_count","Giá trị cực đoan/bất thường","Số phòng tắm bất thường","REVIEW","Kiểm tra theo phân phối dữ liệu"],
]

thresholds=[
    ["Nhà",550,735000000],
    ["Đất",10363.2,347947900],
    ["Căn hộ chung cư",235,261864400],
    ["Biệt thự/Nhà liền kề",900,625000000],
    ["Shophouse",535,552310400],
]

profiling=[
    ["total_rows",3500744,"Tổng số bản ghi"],
    ["null_price",218494,"Số bản ghi thiếu giá"],
    ["zero_price",57320,"Số bản ghi có giá bằng 0"],
    ["invalid_price",0,"Price không cast được sang số"],
    ["null_area",4,"Số bản ghi thiếu diện tích"],
    ["duplicate_groups",36,"Số nhóm duplicate"],
    ["duplicate_records",50,"Số bản ghi duplicate dư"],
    ["median_area",80,"Median diện tích toàn bộ dataset"],
    ["p99_area",2222,"P99 diện tích toàn bộ dataset"],
    ["max_area",82000000,"Diện tích lớn nhất quan sát được"],
    ["median_price",7300000000,"Median giá"],
    ["p99_price",136000000000,"P99 giá"],
    ["max_price",26597191000000000,"Giá lớn nhất quan sát được"],
    ["median_price_per_m2",97777780,"Median giá/m²"],
    ["p99_price_per_m2",607692300,"P99 giá/m²"],
]

wb=Workbook()

# -------------------------
# Sheet 1: DQ Rules
# -------------------------
ws=wb.active
ws.title="dq_rules"

headers=[
    "rule_id",
    "column",
    "condition",
    "description",
    "dq_status",
    "action"
]

ws.append(headers)

for row in rules:
    ws.append(row)

# -------------------------
# Sheet 2: Thresholds
# -------------------------
ws2=wb.create_sheet("property_type_thresholds")

ws2.append([
    "property_type",
    "p99_area_m2",
    "p99_price_per_m2"
])

for row in thresholds:
    ws2.append(row)

# -------------------------
# Sheet 3: Profiling baseline
# -------------------------
ws3=wb.create_sheet("profiling_baseline")

ws3.append([
    "metric",
    "value",
    "description"
])

for row in profiling:
    ws3.append(row)

# -------------------------
# Format
# -------------------------
header_fill=PatternFill("solid",fgColor="1F4E78")
header_font=Font(color="FFFFFF",bold=True)

thin=Side(style="thin",color="B7B7B7")

for sheet in wb.worksheets:

    for cell in sheet[1]:
        cell.fill=header_fill
        cell.font=header_font
        cell.alignment=Alignment(
            horizontal="center",
            vertical="center"
        )

    for row in sheet.iter_rows():
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

    sheet.freeze_panes="A2"
    sheet.auto_filter.ref=sheet.dimensions

# Width sheet 1
widths={
    "A":12,
    "B":22,
    "C":42,
    "D":42,
    "E":15,
    "F":48
}

for col,width in widths.items():
    ws.column_dimensions[col].width=width

# Width sheet 2
ws2.column_dimensions["A"].width=28
ws2.column_dimensions["B"].width=20
ws2.column_dimensions["C"].width=25

# Width sheet 3
ws3.column_dimensions["A"].width=28
ws3.column_dimensions["B"].width=22
ws3.column_dimensions["C"].width=45

os.makedirs(os.path.dirname(OUTPUT_PATH),exist_ok=True)

wb.save(OUTPUT_PATH)

print("DATA QUALITY RULES CREATED")
print("Output:",OUTPUT_PATH)
print("DQ Rules:",len(rules))