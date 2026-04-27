import pandas as pd
import numpy as np
import uuid
import random
import string
from datetime import date, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# =========================================================
# 0) CONFIG
# =========================================================
PROJECT_ID = "prj-ai-dev-qic"  # <-- your project
client = bigquery.Client(project=PROJECT_ID)

# Date range (100-ish days)
dates = pd.date_range("2025-12-01", "2026-03-10")
months = pd.date_range("2025-12-01", "2026-03-01", freq="MS")
d1 = date(2025, 12, 1)
num_days = len(dates)

np.random.seed(42)
random.seed(42)

print(f"Generating {num_days}-day causal dataset...")

# =========================================================
# 1) HELPERS
# =========================================================
def gen_ids(prefix, n):
    return [f"{prefix}_{str(i).zfill(3)}" for i in range(1, n + 1)]

def rand_str(prefix, k=8):
    return f"{prefix}_" + "".join(random.choices(string.ascii_uppercase + string.digits, k=k))

def make_phone():
    return "05" + "".join(random.choices(string.digits, k=8))

def make_email(prefix):
    return f"{prefix.lower()}{np.random.randint(1000,9999)}@example.sa"

def pick_series(s, n=None, p=None):
    arr = s.dropna().tolist()
    if n is None:
        return random.choice(arr)
    return np.random.choice(arr, size=n, replace=True, p=p)

def bq_cast_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    BigQuery-friendly casting:
      - datetime64 cols: keep as TIMESTAMP if *_TS / contains TS/TIME, else DATE
      - leave python date as DATE
    """
    out = df.copy()
    for c in out.columns:
        if np.issubdtype(out[c].dtype, np.datetime64):
            up = c.upper()
            if up.endswith("_TS") or "TS" in up or "TIME" in up:
                out[c] = pd.to_datetime(out[c])
            else:
                out[c] = pd.to_datetime(out[c]).dt.date
    return out

def ensure_dataset(project_id: str, dataset_id: str, location="US"):
    ds_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    ds_ref.location = location
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        client.create_dataset(ds_ref)
        print(f"✅ Created dataset {dataset_id} ({location})")

# =========================================================
# 2) CAUSAL DRIVERS
# =========================================================
is_weekend = dates.weekday.isin([4, 5])  # Saudi weekend Fri/Sat
is_festival = (dates >= "2025-12-15") & (dates <= "2025-12-31")
temps = 22 - 6 * np.sin(np.pi * np.arange(num_days) / 90) + np.random.normal(0, 2, num_days)
is_rain = np.random.choice([False, True], num_days, p=[0.92, 0.08])

# =========================================================
# 3) DIMENSIONS (RICHER + MORE ROWS)
# =========================================================

# -------------------------
# DIM: Location
# -------------------------
loc_rows = [
    {"Location_ID":"LOC_QID","Country":"Saudi Arabia","Region":"Riyadh","City":"Qiddiya","District":"Entertainment",
     "Latitude":24.58,"Longitude":46.31,"Location_Type":"Destination","Timezone":"Asia/Riyadh",
     "Postal_Code":"00000","Address_Line1":"Qiddiya City","Is_Active":True,"Source_System":"MDM"},
    {"Location_ID":"LOC_RUH","Country":"Saudi Arabia","Region":"Riyadh","City":"Riyadh","District":"Downtown",
     "Latitude":24.71,"Longitude":46.67,"Location_Type":"City","Timezone":"Asia/Riyadh",
     "Postal_Code":"11564","Address_Line1":"Riyadh Center","Is_Active":True,"Source_System":"MDM"},
    {"Location_ID":"LOC_JED","Country":"Saudi Arabia","Region":"Makkah","City":"Jeddah","District":"Corniche",
     "Latitude":21.54,"Longitude":39.17,"Location_Type":"City","Timezone":"Asia/Riyadh",
     "Postal_Code":"21442","Address_Line1":"Jeddah Corniche","Is_Active":True,"Source_System":"MDM"},
    {"Location_ID":"LOC_DMM","Country":"Saudi Arabia","Region":"Eastern","City":"Dammam","District":"Business",
     "Latitude":26.42,"Longitude":50.09,"Location_Type":"City","Timezone":"Asia/Riyadh",
     "Postal_Code":"32241","Address_Line1":"Dammam Center","Is_Active":True,"Source_System":"MDM"},
]
for i in range(5, 21):
    loc_rows.append({
        "Location_ID": f"LOC_{str(i).zfill(3)}",
        "Country":"Saudi Arabia",
        "Region": np.random.choice(["Riyadh","Makkah","Eastern","Asir","Madinah"]),
        "City": np.random.choice(["Riyadh","Jeddah","Dammam","Abha","Madinah"]),
        "District": np.random.choice(["Entertainment","Business","Residential","Industrial"]),
        "Latitude": float(np.round(np.random.uniform(16.5, 28.5), 5)),
        "Longitude": float(np.round(np.random.uniform(34.5, 50.0), 5)),
        "Location_Type": np.random.choice(["City","Venue","Destination"]),
        "Timezone":"Asia/Riyadh",
        "Postal_Code": str(np.random.randint(10000, 99999)),
        "Address_Line1": rand_str("Addr", 10),
        "Is_Active": True,
        "Source_System": "MDM"
    })
dim_location = pd.DataFrame(loc_rows)

# -------------------------
# DIM: Asset
# -------------------------
asset_rows = [
    {"Asset_ID":"AST_SF","Asset_Name":"Six Flags","Asset_Type":"ThemePark","Operator":"Six Flags","Owner":"QIC",
     "Open_Date":date(2024,1,1),"Location_ID":"LOC_QID","Capacity_Daily":30000,"Status":"Operating",
     "Safety_Cert_Level":"A","Is_Ticketed":True,"Brand_Tier":"Premium","Source_System":"EAM"},
    {"Asset_ID":"AST_AA","Asset_Name":"Aqua Arabia","Asset_Type":"WaterPark","Operator":"QIC","Owner":"QIC",
     "Open_Date":date(2024,1,1),"Location_ID":"LOC_QID","Capacity_Daily":25000,"Status":"Operating",
     "Safety_Cert_Level":"A","Is_Ticketed":True,"Brand_Tier":"Premium","Source_System":"EAM"},
    {"Asset_ID":"AST_AMC","Asset_Name":"AMC Riyadh","Asset_Type":"Cinema","Operator":"AMC","Owner":"QIC",
     "Open_Date":date(2024,1,1),"Location_ID":"LOC_RUH","Capacity_Daily":8000,"Status":"Operating",
     "Safety_Cert_Level":"A","Is_Ticketed":True,"Brand_Tier":"Standard","Source_System":"EAM"},
]
for i in range(4, 31):
    asset_rows.append({
        "Asset_ID": f"AST_{str(i).zfill(3)}",
        "Asset_Name": f"QIC Asset {i}",
        "Asset_Type": np.random.choice(["ThemePark","WaterPark","Cinema","Golf","Stadium","Retail","Resort"]),
        "Operator": np.random.choice(["QIC","ThirdParty"], p=[0.7,0.3]),
        "Owner": "QIC",
        "Open_Date": date(2024,1,1) + timedelta(days=int(np.random.randint(0, 700))),
        "Location_ID": pick_series(dim_location["Location_ID"]),
        "Capacity_Daily": int(np.random.randint(1000, 45000)),
        "Status": np.random.choice(["Operating","UnderConstruction","Planned"], p=[0.7,0.2,0.1]),
        "Safety_Cert_Level": np.random.choice(["A","B","C"], p=[0.7,0.25,0.05]),
        "Is_Ticketed": bool(np.random.choice([True,False], p=[0.8,0.2])),
        "Brand_Tier": np.random.choice(["Premium","Standard","Value"], p=[0.4,0.5,0.1]),
        "Source_System":"EAM",
    })
dim_asset = pd.DataFrame(asset_rows)

# -------------------------
# DIM: Project
# -------------------------
proj_rows = []
for i in range(1, 26):
    proj_rows.append({
        "Project_ID": f"PRJ_{str(i).zfill(3)}",
        "Project_Name": f"Project {i}",
        "Project_Type": np.random.choice(["Construction","IT","Marketing","Operations"]),
        "Asset_ID": pick_series(dim_asset["Asset_ID"]),
        "Start_Date": date(2025,1,1) + timedelta(days=int(np.random.randint(0, 220))),
        "End_Date": date(2026,12,31) + timedelta(days=int(np.random.randint(-60, 180))),
        "Status": np.random.choice(["Active","Planned","Completed"], p=[0.6,0.3,0.1]),
        "Budget_SAR": float(np.round(np.random.uniform(2e5, 2e7), 2)),
        "Program_Name": np.random.choice(["Qiddiya Growth","Experience Excellence","Digital Transformation"]),
        "Sponsor": np.random.choice(["COO","CFO","CTO","CEO"]),
        "Source_System": "PMO"
    })
dim_project = pd.DataFrame(proj_rows)

# -------------------------
# DIM: Currency
# -------------------------
dim_currency = pd.DataFrame([
    {"Currency_Code":"SAR","Currency_Name":"Saudi Riyal","Is_Base_Currency":True,"Symbol":"﷼","Minor_Unit":2,"ISO_Num":682},
    {"Currency_Code":"USD","Currency_Name":"US Dollar","Is_Base_Currency":False,"Symbol":"$","Minor_Unit":2,"ISO_Num":840},
    {"Currency_Code":"EUR","Currency_Name":"Euro","Is_Base_Currency":False,"Symbol":"€","Minor_Unit":2,"ISO_Num":978},
])

# -------------------------
# DIM: HR
# -------------------------
dim_department = pd.DataFrame([
    {"Department_ID":"DEP_HR","Department_Name":"HR","Parent_Department_ID":None,"Business_Unit":"Corp","Is_Active":True,"Cost_Center_ID":"CC_HR","VP_Name":"VP People"},
    {"Department_ID":"DEP_OP","Department_Name":"Operations","Parent_Department_ID":None,"Business_Unit":"Corp","Is_Active":True,"Cost_Center_ID":"CC_OP","VP_Name":"VP Ops"},
    {"Department_ID":"DEP_FIN","Department_Name":"Finance","Parent_Department_ID":None,"Business_Unit":"Corp","Is_Active":True,"Cost_Center_ID":"CC_FIN","VP_Name":"VP Finance"},
    {"Department_ID":"DEP_IT","Department_Name":"IT","Parent_Department_ID":None,"Business_Unit":"Corp","Is_Active":True,"Cost_Center_ID":"CC_IT","VP_Name":"VP Technology"},
])

dim_job = pd.DataFrame([
    {"Job_ID":"JOB_MGR","Job_Title":"Manager","Job_Level":"L3","Job_Family":"Ops","Job_Track":"Management","Is_Shift_Based":False},
    {"Job_ID":"JOB_SUP","Job_Title":"Supervisor","Job_Level":"L2","Job_Family":"Ops","Job_Track":"Management","Is_Shift_Based":True},
    {"Job_ID":"JOB_STF","Job_Title":"Staff","Job_Level":"L1","Job_Family":"Ops","Job_Track":"Individual","Is_Shift_Based":True},
    {"Job_ID":"JOB_ANA","Job_Title":"Analyst","Job_Level":"L2","Job_Family":"Corporate","Job_Track":"Individual","Is_Shift_Based":False},
])

dim_comp_band = pd.DataFrame([
    {"Band_ID":"BND_1","Job_Level":"L3","Min_Salary":15000,"Mid_Salary":20000,"Max_Salary":25000,"Currency":"SAR","Bonus_Pct":0.15,"Allowance_Default":2500},
    {"Band_ID":"BND_2","Job_Level":"L2","Min_Salary":9000,"Mid_Salary":12000,"Max_Salary":16000,"Currency":"SAR","Bonus_Pct":0.08,"Allowance_Default":1500},
    {"Band_ID":"BND_3","Job_Level":"L1","Min_Salary":4000,"Mid_Salary":6000,"Max_Salary":8000,"Currency":"SAR","Bonus_Pct":0.03,"Allowance_Default":1000},
])

emp_n = 200
emp_ids = gen_ids("EMP", emp_n)

dim_employee = pd.DataFrame({
    "Employee_ID": emp_ids,
    "National_ID_Hash": [uuid.uuid4().hex[:16] for _ in range(emp_n)],
    "First_Name": [f"Emp{i}" for i in range(1, emp_n+1)],
    "Last_Name": np.random.choice(["AlQahtani","AlHarbi","AlOtaibi","Khan","Raj","Nasser","Fahad"], emp_n),
    "Email": [make_email(f"emp{i}") for i in range(1, emp_n+1)],
    "Phone": [make_phone() for _ in range(emp_n)],
    "Gender": np.random.choice(["Male","Female"], emp_n, p=[0.7,0.3]),
    "Nationality_Code": np.random.choice(["SA","IN","EG","PH","PK"], emp_n, p=[0.6,0.15,0.1,0.1,0.05]),
})
dim_employee["Is_Saudi"] = dim_employee["Nationality_Code"].eq("SA")
dim_employee["Hire_Date"] = [date(2023,1,1) + timedelta(days=int(np.random.randint(0, 900))) for _ in range(emp_n)]
dim_employee["Termination_Date"] = None
dim_employee["Employment_Status"] = "Active"
dim_employee["Department_ID"] = pick_series(dim_department["Department_ID"], emp_n, p=[0.15,0.55,0.2,0.1])
dim_employee["Job_ID"] = pick_series(dim_job["Job_ID"], emp_n, p=[0.12,0.22,0.5,0.16])

job_level_map = dim_job.set_index("Job_ID")["Job_Level"].to_dict()
band_map = dim_comp_band.set_index("Job_Level")["Band_ID"].to_dict()
dim_employee["Job_Level"] = dim_employee["Job_ID"].map(job_level_map)
dim_employee["Band_ID"] = dim_employee["Job_Level"].map(band_map)

band_mid = dim_comp_band.set_index("Band_ID")["Mid_Salary"].to_dict()
band_allow = dim_comp_band.set_index("Band_ID")["Allowance_Default"].to_dict()

dim_employee["Base_Salary_Monthly"] = np.round(dim_employee["Band_ID"].map(band_mid) * np.random.uniform(0.8, 1.2, emp_n), 2)
dim_employee["Allowance_Monthly"] = dim_employee["Band_ID"].map(band_allow).astype(float)
dim_employee["Currency"] = "SAR"
dim_employee["Payroll_System"] = np.random.choice(["SAP","Oracle"], emp_n, p=[0.8,0.2])
dim_employee["Last_Updated_TS"] = pd.Timestamp.now()

# -------------------------
# DIM: Finance
# -------------------------
dim_gl_account = pd.DataFrame({
    "GL_Account_ID": ["GL_REV_SF", "GL_REV_CIN", "GL_REV_FNB", "GL_EXP_PAY", "GL_EXP_VEND", "GL_EXP_UTIL"],
    "GL_Account_Name": ["Themepark Rev", "Cinema Rev", "FNB Rev", "Payroll", "Vendor Exp", "Utilities"],
    "Account_Type": ["Revenue","Revenue","Revenue","Expense","Expense","Expense"],
    "Account_Group": ["P&L"]*6
})

dim_cost_center = pd.DataFrame([
    {"Cost_Center_ID":"CC_OP","Cost_Center_Name":"Operations","Department_ID":"DEP_OP"},
    {"Cost_Center_ID":"CC_FIN","Cost_Center_Name":"Finance","Department_ID":"DEP_FIN"},
    {"Cost_Center_ID":"CC_HR","Cost_Center_Name":"HR","Department_ID":"DEP_HR"},
    {"Cost_Center_ID":"CC_IT","Cost_Center_Name":"IT","Department_ID":"DEP_IT"},
])

# -------------------------
# DIM: Procurement / Payments
# -------------------------
ven_n = 30
dim_vendor = pd.DataFrame({
    "Vendor_ID": gen_ids("VEN", ven_n),
    "Vendor_Name": [f"Vendor {i}" for i in range(1, ven_n+1)],
    "Vendor_Type": np.random.choice(["Supplier","Contractor","Consultant"], ven_n, p=[0.6,0.3,0.1]),
    "Country": "SA",
    "Is_Local_Saudi": np.random.choice([True,False], ven_n, p=[0.8,0.2]),
    "Risk_Rating": np.random.choice(["Low","Medium","High"], ven_n, p=[0.6,0.3,0.1]),
    "Preferred_Vendor_Flag": np.random.choice([True,False], ven_n, p=[0.7,0.3]),
    "Last_Updated_TS": pd.Timestamp.now()
})

item_n = 80
dim_item = pd.DataFrame({
    "Item_ID": gen_ids("ITM", item_n),
    "Item_Name": [f"Item {i}" for i in range(1, item_n+1)],
    "Category": np.random.choice(["Materials","Services","IT","F&B"], item_n, p=[0.35,0.35,0.2,0.1]),
    "UOM": np.random.choice(["EA","HRS","KG","BOX"], item_n)
})

dim_payment_terms = pd.DataFrame([
    {"Payment_Terms_Code":"NET15","Terms_Days":15},
    {"Payment_Terms_Code":"NET30","Terms_Days":30},
    {"Payment_Terms_Code":"NET60","Terms_Days":60},
])

dim_rejection_reason = pd.DataFrame([
    {"Reason_Code":"REJ_01","Category":"Docs","Reason_Text":"Missing Docs"},
    {"Reason_Code":"REJ_02","Category":"Price","Reason_Text":"Price Mismatch"},
    {"Reason_Code":"REJ_03","Category":"Tax","Reason_Text":"Tax Mismatch"},
])

# -------------------------
# DIM: Six Flags-like (themepark asset)
# -------------------------
themepark_assets = dim_asset[dim_asset["Asset_Type"].eq("ThemePark")].copy()
sf_asset_id = "AST_SF" if "AST_SF" in themepark_assets["Asset_ID"].values else themepark_assets.iloc[0]["Asset_ID"]

dim_gate = pd.DataFrame([
    {"Gate_ID":"GT_MAIN","Gate_Name":"Main Gate","Asset_ID":sf_asset_id,"Gate_Type":"Entry","Is_Active":True},
    {"Gate_ID":"GT_VIP","Gate_Name":"VIP Gate","Asset_ID":sf_asset_id,"Gate_Type":"Entry","Is_Active":True},
    {"Gate_ID":"GT_STAFF","Gate_Name":"Staff Gate","Asset_ID":sf_asset_id,"Gate_Type":"Staff","Is_Active":True},
])

ride_n = 25
dim_ride = pd.DataFrame({
    "Ride_ID": gen_ids("RD", ride_n),
    "Ride_Name": [f"Ride {i}" for i in range(1, ride_n+1)],
    "Ride_Category": np.random.choice(["Thrill","Family","Kids"], ride_n, p=[0.5,0.35,0.15]),
    "Capacity_Per_Cycle": np.random.choice([16,24,32,40], ride_n),
    "Average_Cycle_Min": np.round(np.random.uniform(2.0, 6.0, ride_n), 2),
    "Is_Operational": True,
    "Asset_ID": sf_asset_id,
    "Manufacturer": np.random.choice(["Intamin","B&M","Vekoma","Mack"], ride_n),
    "Install_Year": np.random.choice([2024,2025,2026], ride_n, p=[0.5,0.35,0.15]),
})

out_n = 20
dim_outlet = pd.DataFrame({
    "Outlet_ID": gen_ids("OUT", out_n),
    "Outlet_Name": [f"Outlet {i}" for i in range(1, out_n+1)],
    "Outlet_Type": np.random.choice(["F&B","Retail","Kiosk"], out_n, p=[0.6,0.3,0.1]),
    "Asset_ID": sf_asset_id,
    "Is_Active": True,
    "Open_Hour": np.random.choice([10,11,12], out_n),
    "Close_Hour": np.random.choice([20,21,22,23], out_n),
})

prod_n = 80
dim_product = pd.DataFrame({
    "Product_ID": gen_ids("PRD", prod_n),
    "Product_Name": [f"Product {i}" for i in range(1, prod_n+1)],
    "Category": np.random.choice(["Food","Beverage","Merch"], prod_n, p=[0.45,0.25,0.30]),
    "Base_Price": np.round(np.random.uniform(10, 250, prod_n), 2),
    "Is_Active": True,
    "Brand": np.random.choice(["QIC","Partner"], prod_n, p=[0.7,0.3]),
})

dim_ticket_product = pd.DataFrame([
    {"Ticket_Product_ID":"TK_DAY","Ticket_Product_Name":"Day Pass","Ticket_Product_Type":"DayPass","Base_Price":250.0,"Asset_ID":sf_asset_id,"Is_Active":True},
    {"Ticket_Product_ID":"TK_VIP","Ticket_Product_Name":"VIP Pass","Ticket_Product_Type":"VIP","Base_Price":600.0,"Asset_ID":sf_asset_id,"Is_Active":True},
    {"Ticket_Product_ID":"TK_FAM","Ticket_Product_Name":"Family Pack","Ticket_Product_Type":"Bundle","Base_Price":850.0,"Asset_ID":sf_asset_id,"Is_Active":True},
])

# -------------------------
# DIM: AMC-like (cinema asset)
# -------------------------
cinema_assets = dim_asset[dim_asset["Asset_Type"].eq("Cinema")].copy()
amc_asset_id = "AST_AMC" if "AST_AMC" in cinema_assets["Asset_ID"].values else cinema_assets.iloc[0]["Asset_ID"]

dim_cinema = pd.DataFrame([{
    "Cinema_ID":"CIN_01",
    "Cinema_Name":"AMC Riyadh",
    "City":"Riyadh",
    "Location_ID":"LOC_RUH",
    "Screens": 12,
    "Premium_Formats":"IMAX",
    "Asset_ID": amc_asset_id,
    "Is_Active": True
}])

film_n = 30
dim_film = pd.DataFrame({
    "Film_ID": gen_ids("FLM", film_n),
    "Film_Title": [f"Film {i}" for i in range(1, film_n+1)],
    "Distributor": np.random.choice(["WB","Disney","Universal","Sony"], film_n),
    "Genre": np.random.choice(["Sci-Fi","Action","Comedy","Drama","Animation"], film_n),
    "Release_Date": [date(2025,10,1) + timedelta(days=int(np.random.randint(0, 200))) for _ in range(film_n)],
    "Language": np.random.choice(["English","Arabic","Hindi"], film_n, p=[0.7,0.2,0.1]),
    "Runtime_Min": np.random.choice([90,100,110,120,130,140,150,160], film_n),
})

dim_concession_product = pd.DataFrame({
    "Product_ID": gen_ids("CONC", 20),
    "Product_Name": [f"Concession {i}" for i in range(1, 21)],
    "Category": np.random.choice(["Food","Beverage"], 20, p=[0.6,0.4]),
    "Base_Price": np.round(np.random.uniform(10, 60, 20), 2),
    "Is_Active": True
})

# =========================================================
# 4) SHARED TIME DIM + WEATHER + EVENTS
# =========================================================
dim_date = pd.DataFrame({
    "Date": dates,
    "Year": dates.year,
    "Quarter": dates.quarter,
    "Month": dates.month,
    "Month_Name": dates.month_name(),
    "Week": dates.isocalendar().week.astype(int),
    "Day": dates.day,
    "Day_Name": dates.day_name(),
    "Is_Weekend_Saudi": is_weekend,
    "Is_Month_End": dates.is_month_end,
    "Is_Quarter_End": dates.is_quarter_end,
    "Is_Saudi_Public_Holiday": False,
    "Holiday_Name": None,
    "Is_School_Holiday": False,
    "Is_University_Holiday": False,
    "Is_Ramadan_Flag": False,
    "Season_Tag": "Winter"
})

fact_weather_daily = pd.DataFrame({
    "Date": dates,
    "Location_ID": "LOC_QID",
    "City": "Qiddiya",
    "Temp_Max_C": np.round(temps, 1),
    "Temp_Min_C": np.round(temps - 10, 1),
    "Temp_Avg_C": np.round(temps - 5, 1),
    "Humidity_Avg": np.round(np.random.uniform(30, 60, num_days), 1),
    "Wind_Speed_Avg_KMH": np.round(np.random.uniform(5, 28, num_days), 1),
    "Precip_MM": np.where(is_rain, np.round(np.random.uniform(2, 10, num_days), 1), 0.0),
    "Dust_Index": np.round(np.random.uniform(1, 5, num_days), 1),
    "Heat_Index": np.round(temps, 1),
    "Weather_Condition": np.where(is_rain, "Rain", "Clear"),
    "Is_Extreme_Heat": False,
    "Data_Source": "NCM"
})

fact_event_calendar = pd.DataFrame([{
    "Event_ID": "EVT_WINTER",
    "Event_Name": "Winter Festival",
    "Event_Type": "Cultural",
    "Target_Audience": "All",
    "Start_Date": date(2025,12,15),
    "End_Date": date(2025,12,31),
    "Start_TS": pd.Timestamp("2025-12-15 10:00:00"),
    "End_TS": pd.Timestamp("2025-12-31 23:00:00"),
    "Location_ID": "LOC_QID",
    "City": "Qiddiya",
    "Venue_Name": "Plaza",
    "Is_International": True,
    "Featured_Artist": None,
    "Popularity_Score": 95.0,
    "Expected_Attendance": 50000,
    "Ticketed_Flag": False,
    "Price_Min": 0.0,
    "Price_Max": 0.0,
    "Source_System": "Events",
    "Created_TS": pd.Timestamp.now()
}])

# =========================================================
# 5) VISITOR MODEL PER ASSET (USING DIM ASSETS)
# =========================================================
visitor_assets = dim_asset[dim_asset["Asset_Type"].isin(["ThemePark","WaterPark","Cinema"])].copy()
base_by_type = {"ThemePark": 5000, "WaterPark": 3500, "Cinema": 1500}

def mult_for_type(asset_type: str):
    if asset_type in ["ThemePark","WaterPark"]:
        return (1.5 * is_weekend + 1.0 * ~is_weekend) * (1.3 * is_festival + 1.0 * ~is_festival) * (0.6 * is_rain + 1.0 * ~is_rain)
    if asset_type == "Cinema":
        return (1.4 * is_weekend + 1.0 * ~is_weekend) * (1.1 * is_festival + 1.0 * ~is_festival) * (1.4 * is_rain + 1.0 * ~is_rain)
    return np.ones(num_days)

asset_daily_visitors = {}
for _, a in visitor_assets.iterrows():
    base = base_by_type.get(a["Asset_Type"], 1000)
    mult = mult_for_type(a["Asset_Type"])
    asset_daily_visitors[a["Asset_ID"]] = np.random.poisson(base * mult)

def visitors(asset_id):
    return asset_daily_visitors.get(asset_id, np.random.poisson(500 * np.ones(num_days)))

sf_visitors = visitors(sf_asset_id)
amc_visitors = visitors(amc_asset_id)

# =========================================================
# 6) SIX FLAGS FACTS (STRICTLY USING DIM IDS)
# =========================================================
ticket_products = dim_ticket_product["Ticket_Product_ID"]
gates = dim_gate["Gate_ID"]
rides = dim_ride["Ride_ID"]
outlets = dim_outlet["Outlet_ID"]
products = dim_product["Product_ID"]

# Ticket sales
fact_ticket_sales = pd.DataFrame({
    "Ticket_TXN_ID": gen_ids("SFTXN", num_days),
    "TXN_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(8, 22, num_days), unit="h"),
    "Date": dates,
    "Asset_ID": sf_asset_id,
    "Sales_Channel": np.random.choice(["Online","Onsite","Partner"], num_days, p=[0.55,0.35,0.10]),
    "Ticket_Product_ID": np.random.choice(ticket_products, num_days, p=[0.75,0.15,0.10]),
    "Quantity": sf_visitors,
})
price_map = dim_ticket_product.set_index("Ticket_Product_ID")["Base_Price"].to_dict()
fact_ticket_sales["Gross_Amount"] = np.round(fact_ticket_sales["Quantity"] * fact_ticket_sales["Ticket_Product_ID"].map(price_map), 2)
fact_ticket_sales["Discount_Amount"] = np.round(fact_ticket_sales["Gross_Amount"] * np.where(is_festival, 0.08, 0.03), 2)
fact_ticket_sales["Net_Amount"] = np.round((fact_ticket_sales["Gross_Amount"] - fact_ticket_sales["Discount_Amount"]) * 0.85, 2)
fact_ticket_sales["Tax_Amount"] = np.round((fact_ticket_sales["Gross_Amount"] - fact_ticket_sales["Discount_Amount"]) * 0.15, 2)
fact_ticket_sales["Promotion_Code"] = np.where(is_festival, "WINTER25", None)

# Redemptions
fact_ticket_redemptions = pd.DataFrame({
    "Redemption_ID": fact_ticket_sales["Ticket_TXN_ID"],
    "Ticket_TXN_ID": fact_ticket_sales["Ticket_TXN_ID"],
    "Date": dates,
    "Scan_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(9, 22, num_days), unit="h"),
    "Gate_ID": np.random.choice(gates, num_days, p=[0.75,0.15,0.10]),
    "Entries": fact_ticket_sales["Quantity"]
})

# Gate footfall (hourly-ish)
fact_footfall_gate_hourly = pd.DataFrame({
    "Gate_ID": np.random.choice(gates, num_days, p=[0.75,0.15,0.10]),
    "Hour_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(9, 22, num_days), unit="h"),
    "Date": dates,
    "Entries": sf_visitors,
    "Exits": sf_visitors,
    "In_Park_Estimate": (sf_visitors // 2).astype(int)
})

# Downtime correlates with rain
downtimes = np.where(is_rain, np.random.randint(30, 120, num_days), np.random.randint(0, 20, num_days))
ride_pick = np.random.choice(rides, num_days)

fact_ride_operations_hourly = pd.DataFrame({
    "Ride_ID": ride_pick,
    "Hour_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 20, num_days), unit="h"),
    "Date": dates,
    "Operating_Minutes": np.clip(60 - (downtimes // 10), 0, 60),
    "Downtime_Minutes": (downtimes // 10).astype(int),
    "Downtime_Reason": np.where(is_rain, "Weather", np.random.choice(["Maintenance","Reset","Staffing"], num_days)),
    "Cycles": np.random.randint(6, 16, num_days),
    "Utilization_Pct": np.round(np.random.uniform(55, 95, num_days), 2),
    "Staff_On_Duty": np.random.randint(1, 5, num_days)
})
cap_map = dim_ride.set_index("Ride_ID")["Capacity_Per_Cycle"].to_dict()
fact_ride_operations_hourly["Riders"] = (fact_ride_operations_hourly["Cycles"] * fact_ride_operations_hourly["Ride_ID"].map(cap_map)).astype(int)

fact_ride_downtime_events = pd.DataFrame({
    "Downtime_Event_ID": gen_ids("DWE", num_days),
    "Ride_ID": ride_pick,
    "Start_Date": dates,
    "Start_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 20, num_days), unit="h"),
    "End_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 20, num_days), unit="h") + pd.Timedelta(minutes=15),
    "Downtime_Reason": np.where(is_rain, "Weather", "Reset"),
    "Downtime_Minutes": 15
})

# POS purchases (join dims for consistent descriptive columns)
pos_qty = (sf_visitors * np.random.uniform(0.15, 0.40, num_days)).astype(int)

fact_pos_purchases = pd.DataFrame({
    "POS_Line_ID": gen_ids("POS", num_days),
    "Transaction_ID": gen_ids("TRX", num_days),
    "TXN_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 22, num_days), unit="h"),
    "Date": dates,
    "Outlet_ID": np.random.choice(outlets, num_days),
    "Product_ID": np.random.choice(products, num_days),
    "Quantity": pos_qty
}).merge(
    dim_outlet[["Outlet_ID","Outlet_Type","Asset_ID"]],
    on="Outlet_ID",
    how="left"
).merge(
    dim_product[["Product_ID","Product_Name","Category","Base_Price"]],
    on="Product_ID",
    how="left"
)

fact_pos_purchases["Unit_Price"] = np.round(fact_pos_purchases["Base_Price"] * np.random.uniform(0.9, 1.1, num_days), 2)
fact_pos_purchases["Net_Amount"] = np.round(fact_pos_purchases["Quantity"] * fact_pos_purchases["Unit_Price"], 2)
fact_pos_purchases["Payment_Method"] = np.random.choice(["Card","Cash","Wallet"], num_days, p=[0.65,0.10,0.25])
fact_pos_purchases.drop(columns=["Base_Price"], inplace=True)

# Queue time (per ride per day sample)
fact_queue_time_samples = pd.DataFrame({
    "Ride_ID": ride_pick,
    "Sample_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 22, num_days), unit="h"),
    "Date": dates,
    "Queue_Minutes": np.round((sf_visitors / 120) + np.random.normal(0, 2, num_days) + (is_festival * 5), 2)
})

# ✅ Only clip numeric column(s)
fact_queue_time_samples["Queue_Minutes"] = fact_queue_time_samples["Queue_Minutes"].clip(lower=0)

# Customer satisfaction
csat_scores = 95 - (downtimes / 10) - (sf_visitors / 1200)  # drops if downtime/crowds
fact_customer_satisfaction = pd.DataFrame({
    "Survey_Response_ID": gen_ids("SRV", num_days),
    "Response_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(12, 23, num_days), unit="h"),
    "Date": dates,
    "Channel": np.random.choice(["App","Kiosk","Email"], num_days, p=[0.6,0.25,0.15]),
    "NPS_Score": np.clip(np.round(csat_scores/10).astype(int), 0, 10),
    "CSAT_Score": np.clip(np.round(csat_scores).astype(int), 0, 100),
    "Ride_ID": ride_pick,
    "Outlet_ID": np.where(np.random.rand(num_days) < 0.3, np.random.choice(outlets, num_days), None),
    "Free_Text": None,
    "Sentiment_Score": np.round(csat_scores/100, 3),
    "Issue_Category": np.where(downtimes > 60, "Downtime", np.where(sf_visitors > 9000, "Crowding", None))
})

fact_incidents = pd.DataFrame([{
    "Incident_ID": "INC_01",
    "Incident_Date": date(2026,1,10),
    "Asset_ID": sf_asset_id,
    "Ride_ID": pick_series(dim_ride["Ride_ID"]),
    "Severity": "Low",
    "Category": "Ops",
    "Description": "Minor delay"
}])

# =========================================================
# 7) AMC FACTS (STRICTLY USING DIM IDS)
# =========================================================
cinema_id = "CIN_01"
film_ids = dim_film["Film_ID"].tolist()

# Screenings per day (1 session/day sample)
fact_screenings = pd.DataFrame({
    "Session_ID": gen_ids("SES", num_days),
    "Cinema_ID": cinema_id,
    "Film_ID": np.random.choice(film_ids, num_days),
    "Date": dates,
    "Start_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.choice([12,15,18,21], num_days), unit="h"),
    "Screen_Number": np.random.randint(1, 13, num_days),
    "Session_Format": np.random.choice(["Standard","IMAX","4DX"], num_days, p=[0.7,0.2,0.1]),
    "Capacity_Seats": np.random.choice([120,180,220,300,500], num_days, p=[0.15,0.25,0.25,0.2,0.15])
})

fact_cinema_ticket_sales = pd.DataFrame({
    "Ticket_TXN_ID": gen_ids("AMCT", num_days),
    "TXN_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(10, 22, num_days), unit="h"),
    "Date": dates,
    "Cinema_ID": cinema_id,
    "Film_ID": fact_screenings["Film_ID"],
    "Session_ID": fact_screenings["Session_ID"],
    "Sales_Channel": np.random.choice(["Online","Onsite"], num_days, p=[0.75,0.25]),
    "Quantity": np.clip(amc_visitors, 0, fact_screenings["Capacity_Seats"].values),
})
ticket_price = np.round(np.random.uniform(55, 95, num_days), 2)
fact_cinema_ticket_sales["Gross_Amount"] = np.round(fact_cinema_ticket_sales["Quantity"] * ticket_price, 2)
fact_cinema_ticket_sales["Net_Amount"] = np.round(fact_cinema_ticket_sales["Gross_Amount"] * 0.85, 2)
fact_cinema_ticket_sales["Tax_Amount"] = np.round(fact_cinema_ticket_sales["Gross_Amount"] * 0.15, 2)
fact_cinema_ticket_sales["Promotion_Code"] = np.where(is_festival, "WINTER25", None)

# F&B sales
conc_ids = dim_concession_product["Product_ID"].tolist()
fnb_qty = (amc_visitors * np.random.uniform(0.25, 0.65, num_days)).astype(int)
fact_fnb_sales = pd.DataFrame({
    "FNB_Line_ID": gen_ids("AMCF", num_days),
    "Transaction_ID": gen_ids("TRXA", num_days),
    "TXN_TS": pd.to_datetime(dates) + pd.to_timedelta(np.random.randint(12, 23, num_days), unit="h"),
    "Date": dates,
    "Cinema_ID": cinema_id,
    "Product_ID": np.random.choice(conc_ids, num_days),
    "Quantity": fnb_qty
}).merge(
    dim_concession_product[["Product_ID","Product_Name","Category","Base_Price"]],
    on="Product_ID",
    how="left"
)
fact_fnb_sales["Net_Amount"] = np.round(fact_fnb_sales["Quantity"] * np.round(fact_fnb_sales["Base_Price"] * np.random.uniform(0.9, 1.1, num_days), 2), 2)
fact_fnb_sales.drop(columns=["Base_Price"], inplace=True)

# Occupancy per session
fact_occupancy_by_session = pd.DataFrame({
    "Session_ID": fact_screenings["Session_ID"],
    "Date": dates,
    "Cinema_ID": cinema_id,
    "Film_ID": fact_screenings["Film_ID"],
    "Capacity_Seats": fact_screenings["Capacity_Seats"],
    "Tickets_Sold": fact_cinema_ticket_sales["Quantity"]
})
fact_occupancy_by_session["Occupancy_Pct"] = np.round(np.clip(fact_occupancy_by_session["Tickets_Sold"] / fact_occupancy_by_session["Capacity_Seats"], 0, 1), 4)

sem_amc_sales_overall_daily = pd.DataFrame({
    "Date": dates,
    "Cinema_ID": cinema_id,
    "Tickets_Net_Revenue": fact_cinema_ticket_sales["Net_Amount"],
    "FNB_Net_Revenue": fact_fnb_sales["Net_Amount"],
    "Total_Net_Revenue": fact_cinema_ticket_sales["Net_Amount"] + fact_fnb_sales["Net_Amount"],
    "Admissions": fact_cinema_ticket_sales["Quantity"],
    "Transactions": fact_cinema_ticket_sales["Quantity"]
})

# =========================================================
# 8) HR FACTS (STRICTLY USING DIM EMPLOYEE)
# =========================================================
ot_vec = np.where(is_festival, 200.0, 0.0)

snap = pd.DataFrame({
    "Date": np.repeat(dates, emp_n),
    "Employee_ID": np.tile(dim_employee["Employee_ID"].values, num_days),
}).merge(
    dim_employee[["Employee_ID","Department_ID","Employment_Status","Is_Saudi","Gender"]],
    on="Employee_ID",
    how="left"
)
snap["FTE"] = 1.0
snap["Daily_Total_Cost"] = 300.0 + np.repeat(ot_vec, emp_n)
snap["Snapshot_Source"] = "SAP"
fact_employee_daily_snapshot = snap

fact_payroll_monthly = pd.DataFrame([{
    "Payroll_Month": m,
    "Employee_ID": e,
    "Department_ID": dim_employee.loc[dim_employee["Employee_ID"] == e, "Department_ID"].values[0],
    "Gross_Pay": float(np.round(np.random.uniform(7000, 18000), 2)),
    "Net_Pay": float(np.round(np.random.uniform(6000, 16000), 2)),
    "Deductions": 1000.0,
    "Employer_Contrib": 900.0,
    "Payroll_System": "SAP",
    "Payment_Date": m
} for m in months for e in emp_ids])

fact_performance_appraisal = pd.DataFrame([{
    "Appraisal_ID": f"APR_{e}",
    "Employee_ID": e,
    "Department_ID": dim_employee.loc[dim_employee["Employee_ID"] == e, "Department_ID"].values[0],
    "Cycle_Name": "2025 Annual",
    "Cycle_Start_Date": date(2025,1,1),
    "Cycle_End_Date": date(2025,12,31),
    "Rating": np.random.choice(["Below","Meets","Exceeds"], p=[0.1,0.75,0.15]),
    "Score": float(np.round(np.random.uniform(2.0, 4.0), 2)),
    "Finalized_Date": date(2026,1,15)
} for e in emp_ids])

fact_increment_history = pd.DataFrame([{
    "Increment_ID": f"INC_{e}",
    "Employee_ID": e,
    "Effective_Date": date(2026,1,1),
    "Increment_Type": "Merit",
    "Old_Base_Salary": float(np.round(np.random.uniform(7000, 12000), 2)),
    "New_Base_Salary": float(np.round(np.random.uniform(8000, 15000), 2)),
    "Increment_Percent": float(np.round(np.random.uniform(3.0, 15.0), 2)),
    "Reference_Appraisal_ID": f"APR_{e}"
} for e in emp_ids])

absence_n = min(200, num_days)  # ✅ never exceed available dates

fact_absence_daily = pd.DataFrame({
    "Date": dates[:absence_n],
    "Employee_ID": np.random.choice(emp_ids, absence_n, replace=True),
    "Absence_Type": np.random.choice(["Sick","Vacation","Personal"], absence_n, p=[0.4,0.5,0.1]),
    "Hours_Absent": np.random.choice([2.0,4.0,8.0], absence_n, p=[0.2,0.3,0.5])
})

fact_attrition_events = pd.DataFrame([{
    "Attrition_ID": "ATT_01",
    "Employee_ID": emp_ids[-1],
    "Termination_Date": date(2026,2,1),
    "Exit_Type": "Voluntary",
    "Exit_Reason": "Career"
}])

# =========================================================
# 9) FINANCE FACTS (STRICTLY USING DIM GL + COST CENTER)
# =========================================================
# Daily GL journal lines: 2 per day (revenue + payroll)
rev_gl = "GL_REV_SF"
pay_gl = "GL_EXP_PAY"

fact_gl_journal_lines = pd.DataFrame({
    "Journal_Line_ID": gen_ids("JRN", num_days*2),
    "Journal_ID": np.repeat(gen_ids("J", num_days), 2),
    "Date": np.repeat(dates, 2),
    "GL_Account_ID": np.tile([rev_gl, pay_gl], num_days),
    "Cost_Center_ID": np.random.choice(dim_cost_center["Cost_Center_ID"], num_days*2, p=[0.55,0.2,0.15,0.1]),
    "Project_ID": np.where(np.random.rand(num_days*2) < 0.25, np.random.choice(dim_project["Project_ID"], num_days*2), None),
    "Debit_Amount": np.ravel(list(zip(np.zeros(num_days), np.full(num_days, 15000) + (is_festival * 10000)))),
    "Credit_Amount": np.ravel(list(zip(np.round(sf_visitors * 250.0, 2), np.zeros(num_days)))),
    "Currency": "SAR",
    "Narration": "Daily GL"
})

fact_budget_monthly = pd.DataFrame([{
    "Budget_Month": m,
    "Cost_Center_ID": "CC_OP",
    "GL_Account_ID": rev_gl,
    "Budget_Amount": float(np.round(np.random.uniform(1.5e6, 2.5e6), 2)),
    "Currency": "SAR",
    "Version": "V1"
} for m in months])

fact_forecast_monthly = pd.DataFrame([{
    "Forecast_Month": m,
    "Cost_Center_ID": "CC_OP",
    "GL_Account_ID": rev_gl,
    "Forecast_Version": "FCST_1",
    "Forecast_Amount": float(np.round(np.random.uniform(1.6e6, 2.7e6), 2)),
    "Currency": "SAR"
} for m in months])

fact_cashflow_daily = pd.DataFrame({
    "Date": dates,
    "Cash_In": np.round(fact_ticket_sales["Net_Amount"], 2),
    "Cash_Out": np.round(15000.0 + (is_festival * 8000), 2),
})
fact_cashflow_daily["Net_Cashflow"] = np.round(fact_cashflow_daily["Cash_In"] - fact_cashflow_daily["Cash_Out"], 2)

fact_capex_project_monthly = pd.DataFrame([{
    "Capex_Month": m,
    "Project_ID": np.random.choice(dim_project["Project_ID"]),
    "Capex_Amount": float(np.round(np.random.uniform(2e5, 1.2e6), 2)),
    "Currency": "SAR"
} for m in months])

# =========================================================
# 10) PROCUREMENT & PAYMENTS FACTS (STRICTLY USING DIMS)
# =========================================================
vendor_ids = dim_vendor["Vendor_ID"].tolist()
item_ids = dim_item["Item_ID"].tolist()
project_ids = dim_project["Project_ID"].tolist()

# Contracts (few)
fact_contracts = pd.DataFrame([{
    "Contract_ID": "CTR_01",
    "Contract_Number": "C-1",
    "Vendor_ID": vendor_ids[0],
    "Project_ID": project_ids[0],
    "Contract_Type": "EPC",
    "Award_Date": d1,
    "Start_Date": d1,
    "End_Date": date(2026,12,31),
    "Contract_Value": 5000000.0,
    "Currency": "SAR",
    "Status": "Active"
}])

fact_contract_milestones = pd.DataFrame([{
    "Milestone_ID": "MIL_01",
    "Contract_ID": "CTR_01",
    "Milestone_Name": "Kickoff",
    "Planned_Date": d1,
    "Actual_Date": d1,
    "Progress_Pct": 100.0,
    "Status": "Completed"
}])

fact_contract_variations = pd.DataFrame([{
    "Variation_ID": "VAR_01",
    "Contract_ID": "CTR_01",
    "Variation_Date": date(2026,1,15),
    "Variation_Amount": 50000.0,
    "Reason": "Scope",
    "Approved_Flag": True
}])

fact_tender_pipeline = pd.DataFrame([{
    "Tender_ID": "TND_01",
    "Created_Date": d1,
    "Project_ID": project_ids[0],
    "Category": "Services",
    "Stage": "Awarded",
    "Expected_Value": 5000000.0,
    "Awarded_Contract_ID": "CTR_01"
}])

fact_vendor_performance_monthly = pd.DataFrame([{
    "Perf_Month": m,
    "Vendor_ID": vendor_ids[0],
    "OnTime_Delivery_Pct": float(np.round(np.random.uniform(90, 99.5), 2)),
    "Quality_Issue_Rate": float(np.round(np.random.uniform(0.3, 3.0), 2)),
    "Avg_Delay_Days": float(np.round(np.random.uniform(0, 5.0), 2)),
    "Performance_Score": float(np.round(np.random.uniform(75, 98), 2))
} for m in months])

# Causal spend surge: early Dec big POs
po_amts = np.where((dates.month == 12) & (dates.day < 15),
                   np.random.uniform(100000, 300000, num_days),
                   np.random.uniform(5000, 15000, num_days))

fact_purchase_orders = pd.DataFrame({
    "PO_Line_ID": gen_ids("POL", num_days),
    "PO_ID": gen_ids("PO", num_days),
    "PO_Number": gen_ids("PN", num_days),
    "PO_Date": dates,
    "Vendor_ID": np.random.choice(vendor_ids, num_days),
    "Contract_ID": "CTR_01",
    "Project_ID": np.random.choice(project_ids, num_days),
    "Item_ID": np.random.choice(item_ids, num_days),
    "Quantity": np.random.choice([1,2,3,5], num_days, p=[0.6,0.2,0.15,0.05]).astype(float),
    "Unit_Price": np.round(po_amts, 2),
})
fact_purchase_orders["Line_Amount"] = np.round(fact_purchase_orders["Quantity"] * fact_purchase_orders["Unit_Price"], 2)
fact_purchase_orders["Tax_Amount"] = np.round(fact_purchase_orders["Line_Amount"] * 0.15, 2)
fact_purchase_orders["Total_Line_Amount"] = np.round(fact_purchase_orders["Line_Amount"] + fact_purchase_orders["Tax_Amount"], 2)
fact_purchase_orders["Currency"] = "SAR"
fact_purchase_orders["PO_Status"] = np.random.choice(["Approved","Closed","Cancelled"], num_days, p=[0.85,0.10,0.05])

fact_goods_receipt_lines = pd.DataFrame({
    "GRN_Line_ID": gen_ids("GRN", num_days),
    "PO_Line_ID": fact_purchase_orders["PO_Line_ID"],
    "Receipt_Date": dates + pd.Timedelta(days=2),
    "Vendor_ID": fact_purchase_orders["Vendor_ID"],
    "Received_Qty": fact_purchase_orders["Quantity"],
    "Rejected_Qty": np.where(np.random.rand(num_days) < 0.03, 1.0, 0.0),
    "Quality_Flag": np.random.choice([False, True], num_days, p=[0.96,0.04])
})

fact_work_orders = pd.DataFrame({
    "Work_Order_ID": gen_ids("WO", 50),
    "Work_Order_Number": gen_ids("WN", 50),
    "Contract_ID": "CTR_01",
    "Vendor_ID": np.random.choice(vendor_ids, 50),
    "Asset_ID": sf_asset_id,
    "Request_Date": dates[:50],
    "Planned_Start_Date": dates[:50],
    "Planned_End_Date": dates[:50] + pd.Timedelta(days=5),
    "Actual_End_Date": dates[:50] + pd.Timedelta(days=5),
    "Work_Order_Status": np.random.choice(["Closed","Open","Cancelled"], 50, p=[0.75,0.2,0.05]),
    "Approved_Cost": np.round(np.random.uniform(2000, 25000, 50), 2)
})

# AP invoices: 5 days after PO, due NET30 by default
inv_dates = dates + pd.Timedelta(days=5)
terms = "NET30"
due_dates = inv_dates + pd.Timedelta(days=30)
statuses = np.where(due_dates <= pd.Timestamp("2026-03-10"), "Paid", "Approved")

fact_invoices = pd.DataFrame({
    "Invoice_ID": gen_ids("INV", num_days),
    "Invoice_Number": gen_ids("IN", num_days),
    "Vendor_ID": fact_purchase_orders["Vendor_ID"],
    "Contract_ID": fact_purchase_orders["Contract_ID"],
    "PO_ID": fact_purchase_orders["PO_ID"],
    "Invoice_Date": inv_dates,
    "Received_Date": inv_dates,
    "Payment_Terms_Code": terms,
    "Due_Date": due_dates,
    "Invoice_Amount": fact_purchase_orders["Line_Amount"],
    "Tax_Amount": fact_purchase_orders["Tax_Amount"],
    "Total_Amount": fact_purchase_orders["Total_Line_Amount"],
    "Currency": "SAR",
    "Invoice_Status": statuses,
    "Approval_Date": inv_dates + pd.Timedelta(days=1),
    "Paid_Date": np.where(statuses == "Paid", due_dates, pd.NaT),
    "Overdue_Days": 0,
    "Created_TS": pd.to_datetime(inv_dates)
})

fact_invoice_lines = pd.DataFrame({
    "Invoice_Line_ID": gen_ids("INVL", num_days),
    "Invoice_ID": fact_invoices["Invoice_ID"],
    "Invoice_Date": fact_invoices["Invoice_Date"],
    "Item_ID": fact_purchase_orders["Item_ID"],
    "Quantity": fact_purchase_orders["Quantity"],
    "Unit_Price": fact_purchase_orders["Unit_Price"],
    "Line_Amount": fact_purchase_orders["Line_Amount"]
})

fact_invoice_rejections = pd.DataFrame([{
    "Invoice_Rejection_ID": "REJ_01",
    "Invoice_ID": "INV_001",
    "Rejected_Date": date(2025,12,7),
    "Reason_Code": "REJ_01",
    "Rejected_By": "AP",
    "Resubmitted_Date": date(2025,12,8)
}])

paid_df = fact_invoices[fact_invoices["Invoice_Status"] == "Paid"].copy().reset_index(drop=True)
fact_payment_transactions = pd.DataFrame({
    "Payment_ID": gen_ids("PAY", len(paid_df)),
    "Invoice_ID": paid_df["Invoice_ID"],
    "Vendor_ID": paid_df["Vendor_ID"],
    "Payment_Date": paid_df["Paid_Date"],
    "Payment_Amount": paid_df["Total_Amount"],
    "Currency": "SAR",
    "Payment_Method": np.random.choice(["Transfer","Card","Cheque"], len(paid_df), p=[0.85,0.1,0.05]),
    "Payment_Status": "Paid",
    "Payment_Reference": "TRX"
})

fact_overdue_payments_daily = pd.DataFrame([{
    "Date": date(2026,3,10),
    "Invoice_ID": "INV_090",
    "Vendor_ID": vendor_ids[0],
    "Due_Date": date(2026,3,1),
    "Outstanding_Amount": 15000.0,
    "Overdue_Days": 9,
    "Aging_Bucket": "0-30",
    "Status": "Overdue"
}])

fact_disputes = pd.DataFrame([{
    "Dispute_ID": "DSP_01",
    "Invoice_ID": "INV_002",
    "Opened_Date": date(2025,12,10),
    "Closed_Date": date(2025,12,12),
    "Status": "Resolved",
    "Dispute_Reason": "Tax Mismatch"
}])

# =========================================================
# 11) BIGQUERY LOAD (ALL TABLES)
# =========================================================
tables_to_load = [
    # Generic
    (dim_date, "qic_generic", "dim_date"),
    (fact_weather_daily, "qic_generic", "fact_weather_daily"),
    (fact_event_calendar, "qic_generic", "fact_event_calendar"),

    # Shared
    (dim_location, "qic_shared", "dim_location"),
    (dim_asset, "qic_shared", "dim_asset"),
    (dim_project, "qic_shared", "dim_project"),
    (dim_currency, "qic_shared", "dim_currency"),

    # HR
    (dim_department, "qic_hr", "dim_department"),
    (dim_job, "qic_hr", "dim_job"),
    (dim_comp_band, "qic_hr", "dim_comp_band"),
    (dim_employee, "qic_hr", "dim_employee"),
    (fact_employee_daily_snapshot, "qic_hr", "fact_employee_daily_snapshot"),
    (fact_payroll_monthly, "qic_hr", "fact_payroll_monthly"),
    (fact_performance_appraisal, "qic_hr", "fact_performance_appraisal"),
    (fact_increment_history, "qic_hr", "fact_increment_history"),
    (fact_absence_daily, "qic_hr", "fact_absence_daily"),
    (fact_attrition_events, "qic_hr", "fact_attrition_events"),

    # Finance
    (dim_gl_account, "qic_finance", "dim_gl_account"),
    (dim_cost_center, "qic_finance", "dim_cost_center"),
    (fact_gl_journal_lines, "qic_finance", "fact_gl_journal_lines"),
    (fact_budget_monthly, "qic_finance", "fact_budget_monthly"),
    (fact_forecast_monthly, "qic_finance", "fact_forecast_monthly"),
    (fact_cashflow_daily, "qic_finance", "fact_cashflow_daily"),
    (fact_capex_project_monthly, "qic_finance", "fact_capex_project_monthly"),

    # Procurement
    (dim_vendor, "qic_dev_procurement", "dim_vendor"),
    (dim_item, "qic_dev_procurement", "dim_item"),
    (fact_contracts, "qic_dev_procurement", "fact_contracts"),
    (fact_contract_milestones, "qic_dev_procurement", "fact_contract_milestones"),
    (fact_contract_variations, "qic_dev_procurement", "fact_contract_variations"),
    (fact_purchase_orders, "qic_dev_procurement", "fact_purchase_orders"),
    (fact_goods_receipt_lines, "qic_dev_procurement", "fact_goods_receipt_lines"),
    (fact_work_orders, "qic_dev_procurement", "fact_work_orders"),
    (fact_tender_pipeline, "qic_dev_procurement", "fact_tender_pipeline"),
    (fact_vendor_performance_monthly, "qic_dev_procurement", "fact_vendor_performance_monthly"),

    # Payments
    (dim_payment_terms, "qic_dev_payments", "dim_payment_terms"),
    (dim_rejection_reason, "qic_dev_payments", "dim_rejection_reason"),
    (fact_invoices, "qic_dev_payments", "fact_invoices"),
    (fact_invoice_lines, "qic_dev_payments", "fact_invoice_lines"),
    (fact_invoice_rejections, "qic_dev_payments", "fact_invoice_rejections"),
    (fact_payment_transactions, "qic_dev_payments", "fact_payment_transactions"),
    (fact_overdue_payments_daily, "qic_dev_payments", "fact_overdue_payments_daily"),
    (fact_disputes, "qic_dev_payments", "fact_disputes"),

    # Six Flags
    (dim_gate, "qic_asset_sixflags", "dim_gate"),
    (dim_ride, "qic_asset_sixflags", "dim_ride"),
    (dim_outlet, "qic_asset_sixflags", "dim_outlet"),
    (dim_product, "qic_asset_sixflags", "dim_product"),
    (dim_ticket_product, "qic_asset_sixflags", "dim_ticket_product"),
    (fact_ticket_sales, "qic_asset_sixflags", "fact_ticket_sales"),
    (fact_ticket_redemptions, "qic_asset_sixflags", "fact_ticket_redemptions"),
    (fact_footfall_gate_hourly, "qic_asset_sixflags", "fact_footfall_gate_hourly"),
    (fact_ride_operations_hourly, "qic_asset_sixflags", "fact_ride_operations_hourly"),
    (fact_ride_downtime_events, "qic_asset_sixflags", "fact_ride_downtime_events"),
    (fact_pos_purchases, "qic_asset_sixflags", "fact_pos_purchases"),
    (fact_queue_time_samples, "qic_asset_sixflags", "fact_queue_time_samples"),
    (fact_customer_satisfaction, "qic_asset_sixflags", "fact_customer_satisfaction"),
    (fact_incidents, "qic_asset_sixflags", "fact_incidents"),

    # AMC
    (dim_cinema, "qic_asset_amc", "dim_cinema"),
    (dim_film, "qic_asset_amc", "dim_film"),
    (fact_screenings, "qic_asset_amc", "fact_screenings"),
    (fact_cinema_ticket_sales, "qic_asset_amc", "fact_cinema_ticket_sales"),
    (dim_concession_product, "qic_asset_amc", "dim_concession_product"),
    (fact_fnb_sales, "qic_asset_amc", "fact_fnb_sales"),
    (fact_occupancy_by_session, "qic_asset_amc", "fact_occupancy_by_session"),
    (sem_amc_sales_overall_daily, "qic_asset_amc", "sem_amc_sales_overall_daily"),
]

# Ensure datasets exist (choose US; change if you want EU)
for _, ds, _ in tables_to_load:
    ensure_dataset(PROJECT_ID, ds, location="US")

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
print(f"\nUploading {len(tables_to_load)} tables to BigQuery...")

for df, ds, tb in tables_to_load:
    try:
        df2 = bq_cast_dates(df)
        client.load_table_from_dataframe(df2, f"{PROJECT_ID}.{ds}.{tb}", job_config=job_config).result()
        print(f"✅ Loaded {ds}.{tb} ({len(df2)} rows)")
    except Exception as e:
        print(f"❌ Failed {ds}.{tb}: {e}")

print("\n🚀 Done! All dims are richer, and all facts use DIM IDs (referential integrity).")