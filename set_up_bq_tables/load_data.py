import pandas as pd
import numpy as np
import uuid
from datetime import date, timedelta
from google.cloud import bigquery

# ==========================================
# 1. SETUP & CAUSAL ENGINE
# ==========================================
PROJECT_ID = "prj-ai-dev-qic" # Ensure this is your GCP Project ID
client = bigquery.Client(project=PROJECT_ID)

dates = pd.date_range("2025-12-01", "2026-03-10")
months = pd.date_range("2025-12-01", "2026-03-01", freq='MS')
d1 = date(2025, 12, 1) # <--- ADD THIS LINE
num_days = len(dates)

np.random.seed(42) # For reproducible causal models

# Causal Vectors (100 days)
is_weekend = dates.weekday.isin([4, 5])
is_festival = (dates >= "2025-12-15") & (dates <= "2025-12-31")
temps = 22 - 6 * np.sin(np.pi * np.arange(num_days) / 90) + np.random.normal(0, 2, num_days)
is_rain = np.random.choice([False, True], num_days, p=[0.92, 0.08])

# Causal Multipliers
sf_mult = (1.5 * is_weekend + 1.0 * ~is_weekend) * (1.3 * is_festival + 1.0 * ~is_festival) * (0.5 * is_rain + 1.0 * ~is_rain)
amc_mult = (1.5 * is_weekend + 1.0 * ~is_weekend) * (1.1 * is_festival + 1.0 * ~is_festival) * (1.5 * is_rain + 1.0 * ~is_rain)

sf_daily_visitors = np.random.poisson(5000 * sf_mult)
amc_daily_visitors = np.random.poisson(1500 * amc_mult)

print("Generating 100-Day Causal Dataset for 64 Tables...")

# Helper to generate IDs
def gen_ids(prefix, n): return [f"{prefix}_{str(i).zfill(3)}" for i in range(1, n+1)]

# ==========================================
# 2. DIMENSIONS (Shared, HR, Proc, Assets)
# ==========================================
dim_location = pd.DataFrame({"Location_ID": ["LOC_QID", "LOC_RUH"], "Country": "Saudi Arabia", "Region": "Riyadh", "City": ["Qiddiya", "Riyadh"], "District": "Entertainment", "Latitude": [24.58, 24.71], "Longitude": [46.31, 46.67], "Location_Type": "City"})
dim_asset = pd.DataFrame({"Asset_ID": ["AST_SF", "AST_AA", "AST_AMC"], "Asset_Name": ["Six Flags", "Aqua Arabia", "AMC Riyadh"], "Asset_Type": ["ThemePark", "WaterPark", "Cinema"], "Operator": ["Six Flags", "QIC", "AMC"], "Owner": "QIC", "Open_Date": date(2024,1,1), "Location_ID": ["LOC_QID", "LOC_QID", "LOC_RUH"]})
dim_project = pd.DataFrame({"Project_ID": ["PRJ_01", "PRJ_02"], "Project_Name": ["SF Expansion", "AMC IT"], "Project_Type": ["Construction", "IT"], "Asset_ID": ["AST_SF", "AST_AMC"], "Start_Date": date(2025,1,1), "End_Date": date(2026,12,31), "Status": "Active"})
dim_currency = pd.DataFrame({"Currency_Code": ["SAR", "USD"], "Currency_Name": ["Saudi Riyal", "US Dollar"], "Is_Base_Currency": [True, False]})

dim_department = pd.DataFrame({"Department_ID": ["DEP_HR", "DEP_OP", "DEP_FIN"], "Department_Name": ["HR", "Operations", "Finance"], "Parent_Department_ID": None, "Business_Unit": "Corp", "Is_Active": True})
dim_job = pd.DataFrame({"Job_ID": ["JOB_MGR", "JOB_STF"], "Job_Title": ["Manager", "Staff"], "Job_Level": ["L3", "L1"], "Job_Family": "Ops"})
dim_comp_band = pd.DataFrame({"Band_ID": ["BND_1", "BND_2"], "Job_Level": ["L3", "L1"], "Min_Salary": [15000, 4000], "Mid_Salary": [20000, 6000], "Max_Salary": [25000, 8000], "Currency": "SAR"})
emp_ids = gen_ids("EMP", 50)
dim_employee = pd.DataFrame({"Employee_ID": emp_ids, "National_ID_Hash": [uuid.uuid4().hex[:8] for _ in range(50)], "Gender": np.random.choice(["Male", "Female"], 50), "Nationality_Code": "SA", "Is_Saudi": np.random.choice([True, False], 50, p=[0.6, 0.4]), "Hire_Date": date(2024,1,1), "Termination_Date": None, "Employment_Status": "Active", "Department_ID": np.random.choice(dim_department["Department_ID"], 50), "Job_ID": np.random.choice(dim_job["Job_ID"], 50), "Band_ID": "BND_2", "Base_Salary_Monthly": np.random.uniform(5000, 15000, 50), "Allowance_Monthly": 1000.0, "Currency": "SAR", "Payroll_System": "SAP", "Last_Updated_TS": pd.Timestamp.now()})

dim_gl_account = pd.DataFrame({"GL_Account_ID": ["GL_REV_SF", "GL_REV_AMC", "GL_EXP_PAY", "GL_EXP_VEND"], "GL_Account_Name": ["SF Rev", "AMC Rev", "Payroll", "Vendor Exp"], "Account_Type": ["Revenue", "Revenue", "Expense", "Expense"], "Account_Group": "P&L"})
dim_cost_center = pd.DataFrame({"Cost_Center_ID": ["CC_OP", "CC_FIN"], "Cost_Center_Name": ["Operations", "Finance"], "Department_ID": ["DEP_OP", "DEP_FIN"]})

ven_ids = gen_ids("VEN", 10)
dim_vendor = pd.DataFrame({"Vendor_ID": ven_ids, "Vendor_Name": [f"Vendor {i}" for i in range(10)], "Vendor_Type": "Supplier", "Country": "SA", "Is_Local_Saudi": True, "Risk_Rating": np.random.choice(["Low", "Medium"], 10), "Preferred_Vendor_Flag": True, "Last_Updated_TS": pd.Timestamp.now()})
dim_item = pd.DataFrame({"Item_ID": gen_ids("ITM", 20), "Item_Name": [f"Item {i}" for i in range(20)], "Category": "Materials", "UOM": "EA"})
dim_payment_terms = pd.DataFrame({"Payment_Terms_Code": ["NET30", "NET60"], "Terms_Days": [30, 60]})
dim_rejection_reason = pd.DataFrame({"Reason_Code": ["REJ_01", "REJ_02"], "Category": ["Docs", "Price"], "Reason_Text": ["Missing Docs", "Price Mismatch"]})

dim_gate = pd.DataFrame({"Gate_ID": ["GT_MAIN", "GT_VIP"], "Gate_Name": ["Main Gate", "VIP Gate"], "Asset_ID": "AST_SF"})
dim_ride = pd.DataFrame({"Ride_ID": gen_ids("RD", 5), "Ride_Name": [f"Ride {i}" for i in range(5)], "Ride_Category": "Thrill", "Capacity_Per_Cycle": 24, "Average_Cycle_Min": 3.0, "Is_Operational": True, "Asset_ID": "AST_SF"})
dim_outlet = pd.DataFrame({"Outlet_ID": ["OUT_FNB", "OUT_RET"], "Outlet_Name": ["Burger Stand", "Gift Shop"], "Outlet_Type": ["F&B", "Retail"], "Asset_ID": "AST_SF"})
dim_product = pd.DataFrame({"Product_ID": ["PRD_BRG", "PRD_SHT"], "Product_Name": ["Burger", "Shirt"], "Category": ["Food", "Merch"], "Base_Price": [45.0, 120.0]})
dim_ticket_product = pd.DataFrame({"Ticket_Product_ID": ["TK_DAY", "TK_VIP"], "Ticket_Product_Name": ["Day Pass", "VIP Pass"], "Ticket_Product_Type": ["DayPass", "VIP"], "Base_Price": [250.0, 600.0], "Asset_ID": "AST_SF"})

dim_cinema = pd.DataFrame({"Cinema_ID": ["CIN_01"], "Cinema_Name": "AMC Riyadh", "City": "Riyadh", "Location_ID": "LOC_RUH", "Screens": 10, "Premium_Formats": "IMAX"})
dim_film = pd.DataFrame({"Film_ID": ["FLM_1", "FLM_2"], "Film_Title": ["Dune", "Batman"], "Distributor": "WB", "Genre": ["Sci-Fi", "Action"], "Release_Date": date(2025,12,1), "Language": "English", "Runtime_Min": [155, 120]})
dim_concession_product = pd.DataFrame({"Product_ID": ["CONC_POP", "CONC_SODA"], "Product_Name": ["Popcorn", "Soda"], "Category": "Food", "Base_Price": [35.0, 20.0]})

# ==========================================
# 3. FACTS: GENERIC & TIME-SERIES
# ==========================================
dim_date = pd.DataFrame({"Date": dates, "Year": dates.year, "Quarter": dates.quarter, "Month": dates.month, "Month_Name": dates.month_name(), "Week": dates.isocalendar().week, "Day": dates.day, "Day_Name": dates.day_name(), "Is_Weekend_Saudi": is_weekend, "Is_Month_End": dates.is_month_end, "Is_Quarter_End": dates.is_quarter_end, "Is_Saudi_Public_Holiday": False, "Holiday_Name": None, "Is_School_Holiday": False, "Is_University_Holiday": False, "Is_Ramadan_Flag": False, "Season_Tag": "Winter"})
fact_weather_daily = pd.DataFrame({"Date": dates, "Location_ID": "LOC_QID", "City": "Qiddiya", "Temp_Max_C": np.round(temps,1), "Temp_Min_C": np.round(temps-10,1), "Temp_Avg_C": np.round(temps-5,1), "Humidity_Avg": 45.0, "Wind_Speed_Avg_KMH": 15.0, "Precip_MM": np.where(is_rain, 5.0, 0.0), "Dust_Index": 2.0, "Heat_Index": np.round(temps,1), "Weather_Condition": np.where(is_rain, "Rain", "Clear"), "Is_Extreme_Heat": False, "Data_Source": "NCM"})
fact_event_calendar = pd.DataFrame([{"Event_ID": "EVT_WINTER", "Event_Name": "Winter Festival", "Event_Type": "Cultural", "Target_Audience": "All", "Start_Date": date(2025,12,15), "End_Date": date(2025,12,31), "Start_TS": pd.Timestamp("2025-12-15 10:00:00"), "End_TS": pd.Timestamp("2025-12-31 23:00:00"), "Location_ID": "LOC_QID", "City": "Qiddiya", "Venue_Name": "Plaza", "Is_International": True, "Featured_Artist": None, "Popularity_Score": 95.0, "Expected_Attendance": 50000, "Ticketed_Flag": False, "Price_Min": 0.0, "Price_Max": 0.0, "Source_System": "Events", "Created_TS": pd.Timestamp.now()}])

# ==========================================
# 4. FACTS: OPERATIONS (Six Flags & AMC)
# ==========================================
# Vectorized Generation (100 days)
fact_ticket_sales = pd.DataFrame({"Ticket_TXN_ID": gen_ids("SFTXN", num_days), "TXN_TS": pd.to_datetime(dates), "Date": dates, "Asset_ID": "AST_SF", "Sales_Channel": np.random.choice(["Online", "Onsite"], num_days), "Ticket_Product_ID": "TK_DAY", "Quantity": sf_daily_visitors, "Gross_Amount": sf_daily_visitors * 250.0, "Discount_Amount": 0.0, "Net_Amount": sf_daily_visitors * 250.0 * 0.85, "Tax_Amount": sf_daily_visitors * 250.0 * 0.15, "Promotion_Code": None})
fact_ticket_redemptions = pd.DataFrame({"Redemption_ID": fact_ticket_sales["Ticket_TXN_ID"], "Ticket_TXN_ID": fact_ticket_sales["Ticket_TXN_ID"], "Date": dates, "Scan_TS": pd.to_datetime(dates), "Gate_ID": "GT_MAIN", "Entries": sf_daily_visitors})
fact_footfall_gate_hourly = pd.DataFrame({"Gate_ID": "GT_MAIN", "Hour_TS": pd.to_datetime(dates), "Date": dates, "Entries": sf_daily_visitors, "Exits": sf_daily_visitors, "In_Park_Estimate": sf_daily_visitors // 2})
# Downtime correlates with rain
downtimes = np.where(is_rain, np.random.randint(30, 120, num_days), np.random.randint(0, 15, num_days))
fact_ride_operations_hourly = pd.DataFrame({"Ride_ID": "RD_001", "Hour_TS": pd.to_datetime(dates), "Date": dates, "Operating_Minutes": 60 - (downtimes//10), "Downtime_Minutes": downtimes//10, "Downtime_Reason": np.where(is_rain, "Weather", "Maintenance"), "Cycles": 10, "Riders": 240, "Utilization_Pct": 80.0, "Staff_On_Duty": 2})
fact_ride_downtime_events = pd.DataFrame({"Downtime_Event_ID": gen_ids("DWE", num_days), "Ride_ID": "RD_001", "Start_Date": dates, "Start_TS": pd.to_datetime(dates), "End_TS": pd.to_datetime(dates) + pd.Timedelta(minutes=15), "Downtime_Reason": "Reset", "Downtime_Minutes": 15})
fact_pos_purchases = pd.DataFrame({"POS_Line_ID": gen_ids("POS", num_days), "Transaction_ID": gen_ids("TRX", num_days), "TXN_TS": pd.to_datetime(dates), "Date": dates, "Outlet_ID": "OUT_FNB", "Outlet_Type": "F&B", "Product_ID": "PRD_BRG", "Product_Name": "Burger", "Category": "Food", "Quantity": sf_daily_visitors // 3, "Unit_Price": 45.0, "Net_Amount": (sf_daily_visitors // 3) * 45.0, "Payment_Method": "Card"})
fact_queue_time_samples = pd.DataFrame({"Ride_ID": "RD_001", "Sample_TS": pd.to_datetime(dates), "Date": dates, "Queue_Minutes": sf_daily_visitors / 100})
csat_scores = 95 - (downtimes / 10) - (sf_daily_visitors / 1000) # CSAT drops if ride breaks or park is too crowded
fact_customer_satisfaction = pd.DataFrame({"Survey_Response_ID": gen_ids("SRV", num_days), "Response_TS": pd.to_datetime(dates), "Date": dates, "Channel": "App", "NPS_Score": np.round(csat_scores/10).astype(int), "CSAT_Score": np.round(csat_scores).astype(int), "Ride_ID": "RD_001", "Outlet_ID": None, "Free_Text": None, "Sentiment_Score": csat_scores/100, "Issue_Category": None})
fact_incidents = pd.DataFrame([{"Incident_ID": "INC_01", "Incident_Date": date(2026,1,10), "Asset_ID": "AST_SF", "Ride_ID": "RD_001", "Severity": "Low", "Category": "Ops", "Description": "Minor delay"}])

fact_screenings = pd.DataFrame({"Session_ID": gen_ids("SES", num_days), "Cinema_ID": "CIN_01", "Film_ID": "FLM_1", "Date": dates, "Start_TS": pd.to_datetime(dates), "Screen_Number": 1, "Session_Format": "IMAX", "Capacity_Seats": 500})
fact_cinema_ticket_sales = pd.DataFrame({"Ticket_TXN_ID": gen_ids("AMCT", num_days), "TXN_TS": pd.to_datetime(dates), "Date": dates, "Cinema_ID": "CIN_01", "Film_ID": "FLM_1", "Session_ID": fact_screenings["Session_ID"], "Sales_Channel": "Online", "Quantity": amc_daily_visitors, "Gross_Amount": amc_daily_visitors * 75.0, "Net_Amount": amc_daily_visitors * 75.0 * 0.85, "Tax_Amount": amc_daily_visitors * 75.0 * 0.15, "Promotion_Code": None})
fact_fnb_sales = pd.DataFrame({"FNB_Line_ID": gen_ids("AMCF", num_days), "Transaction_ID": gen_ids("TRXA", num_days), "TXN_TS": pd.to_datetime(dates), "Date": dates, "Cinema_ID": "CIN_01", "Product_ID": "CONC_POP", "Product_Name": "Popcorn", "Category": "Food", "Quantity": amc_daily_visitors // 2, "Net_Amount": (amc_daily_visitors // 2) * 35.0})
fact_occupancy_by_session = pd.DataFrame({"Session_ID": fact_screenings["Session_ID"], "Date": dates, "Cinema_ID": "CIN_01", "Film_ID": "FLM_1", "Capacity_Seats": 500, "Tickets_Sold": np.clip(amc_daily_visitors, 0, 500), "Occupancy_Pct": np.clip(amc_daily_visitors/500, 0, 1)})
sem_amc_sales_overall_daily = pd.DataFrame({"Date": dates, "Cinema_ID": "CIN_01", "Tickets_Net_Revenue": fact_cinema_ticket_sales["Net_Amount"], "FNB_Net_Revenue": fact_fnb_sales["Net_Amount"], "Total_Net_Revenue": fact_cinema_ticket_sales["Net_Amount"] + fact_fnb_sales["Net_Amount"], "Admissions": amc_daily_visitors, "Transactions": amc_daily_visitors})

# ==========================================
# 5. FACTS: HR & FINANCE
# ==========================================
# 5,000 headcount rows (50 emp * 100 days). Overtime injected during festival.
hr_snap_rows = []
for d, fest in zip(dates, is_festival):
    ot = 200.0 if fest else 0.0
    for e in emp_ids:
        hr_snap_rows.append({"Date": d, "Employee_ID": e, "Department_ID": "DEP_OP", "Employment_Status": "Active", "Is_Saudi": True, "Gender": "Male", "FTE": 1.0, "Daily_Total_Cost": 300.0 + ot, "Snapshot_Source": "SAP"})
fact_employee_daily_snapshot = pd.DataFrame(hr_snap_rows)

fact_payroll_monthly = pd.DataFrame([{"Payroll_Month": m, "Employee_ID": e, "Department_ID": "DEP_OP", "Gross_Pay": 10000.0, "Net_Pay": 9000.0, "Deductions": 1000.0, "Employer_Contrib": 900.0, "Payroll_System": "SAP", "Payment_Date": m} for m in months for e in emp_ids])
fact_performance_appraisal = pd.DataFrame([{"Appraisal_ID": f"APR_{e}", "Employee_ID": e, "Department_ID": "DEP_OP", "Cycle_Name": "2025 Annual", "Cycle_Start_Date": date(2025,1,1), "Cycle_End_Date": date(2025,12,31), "Rating": "Meets", "Score": 3.0, "Finalized_Date": date(2026,1,15)} for e in emp_ids])
fact_increment_history = pd.DataFrame([{"Increment_ID": f"INC_{e}", "Employee_ID": e, "Effective_Date": date(2026,1,1), "Increment_Type": "Merit", "Old_Base_Salary": 9000.0, "New_Base_Salary": 10000.0, "Increment_Percent": 11.1, "Reference_Appraisal_ID": f"APR_{e}"} for e in emp_ids])
fact_absence_daily = pd.DataFrame({"Date": dates[:10], "Employee_ID": emp_ids[:10], "Absence_Type": "Sick", "Hours_Absent": 8.0})
fact_attrition_events = pd.DataFrame({"Attrition_ID": ["ATT_01"], "Employee_ID": [emp_ids[-1]], "Termination_Date": date(2026,2,1), "Exit_Type": "Voluntary", "Exit_Reason": "Career"})

fact_gl_journal_lines = pd.DataFrame({
    "Journal_Line_ID": gen_ids("JRN", num_days*2),
    "Journal_ID": np.repeat(gen_ids("J", num_days), 2),
    "Date": np.repeat(dates, 2),
    "GL_Account_ID": np.tile(["GL_REV_SF", "GL_EXP_PAY"], num_days),
    "Cost_Center_ID": "CC_OP", "Project_ID": None,
    "Debit_Amount": np.ravel(list(zip(np.zeros(num_days), np.full(num_days, 15000) + (is_festival * 10000)))),
    "Credit_Amount": np.ravel(list(zip(sf_daily_visitors * 250.0, np.zeros(num_days)))),
    "Currency": "SAR", "Narration": "Daily GL"
})
fact_budget_monthly = pd.DataFrame([{"Budget_Month": m, "Cost_Center_ID": "CC_OP", "GL_Account_ID": "GL_REV_SF", "Budget_Amount": 2000000.0, "Currency": "SAR", "Version": "V1"} for m in months])
fact_forecast_monthly = pd.DataFrame([{"Forecast_Month": m, "Cost_Center_ID": "CC_OP", "GL_Account_ID": "GL_REV_SF", "Forecast_Version": "FCST_1", "Forecast_Amount": 2100000.0, "Currency": "SAR"} for m in months])
fact_cashflow_daily = pd.DataFrame({"Date": dates, "Cash_In": fact_ticket_sales["Net_Amount"], "Cash_Out": 15000.0, "Net_Cashflow": fact_ticket_sales["Net_Amount"] - 15000.0})
fact_capex_project_monthly = pd.DataFrame([{"Capex_Month": m, "Project_ID": "PRJ_01", "Capex_Amount": 500000.0, "Currency": "SAR"} for m in months])

# ==========================================
# 6. FACTS: PROCUREMENT & AP (Causal Flow)
# ==========================================
fact_contracts = pd.DataFrame([{"Contract_ID": "CTR_01", "Contract_Number": "C-1", "Vendor_ID": ven_ids[0], "Project_ID": "PRJ_01", "Contract_Type": "EPC", "Award_Date": d1, "Start_Date": d1, "End_Date": date(2026,12,31), "Contract_Value": 5000000.0, "Currency": "SAR", "Status": "Active"}])
fact_contract_milestones = pd.DataFrame([{"Milestone_ID": "MIL_01", "Contract_ID": "CTR_01", "Milestone_Name": "Kickoff", "Planned_Date": d1, "Actual_Date": d1, "Progress_Pct": 100.0, "Status": "Completed"}])
fact_contract_variations = pd.DataFrame([{"Variation_ID": "VAR_01", "Contract_ID": "CTR_01", "Variation_Date": date(2026,1,15), "Variation_Amount": 50000.0, "Reason": "Scope", "Approved_Flag": True}])
fact_tender_pipeline = pd.DataFrame([{"Tender_ID": "TND_01", "Created_Date": d1, "Project_ID": "PRJ_01", "Category": "Services", "Stage": "Awarded", "Expected_Value": 5000000.0, "Awarded_Contract_ID": "CTR_01"}])
fact_vendor_performance_monthly = pd.DataFrame([{"Perf_Month": m, "Vendor_ID": ven_ids[0], "OnTime_Delivery_Pct": 98.0, "Quality_Issue_Rate": 1.5, "Avg_Delay_Days": 1.0, "Performance_Score": 92.0} for m in months])

# Causal Spend Surge: Huge POs generated early Dec for the Winter Festival
po_amts = np.where((dates.month == 12) & (dates.day < 15), np.random.uniform(100000, 300000, num_days), np.random.uniform(5000, 15000, num_days))
fact_purchase_orders = pd.DataFrame({"PO_Line_ID": gen_ids("POL", num_days), "PO_ID": gen_ids("PO", num_days), "PO_Number": gen_ids("PN", num_days), "PO_Date": dates, "Vendor_ID": ven_ids[0], "Contract_ID": "CTR_01", "Project_ID": "PRJ_01", "Item_ID": "ITM_001", "Quantity": 1.0, "Unit_Price": po_amts, "Line_Amount": po_amts, "Tax_Amount": po_amts * 0.15, "Total_Line_Amount": po_amts * 1.15, "Currency": "SAR", "PO_Status": "Approved"})
fact_goods_receipt_lines = pd.DataFrame({"GRN_Line_ID": gen_ids("GRN", num_days), "PO_Line_ID": fact_purchase_orders["PO_Line_ID"], "Receipt_Date": dates + pd.Timedelta(days=2), "Vendor_ID": ven_ids[0], "Received_Qty": 1.0, "Rejected_Qty": 0.0, "Quality_Flag": False})
fact_work_orders = pd.DataFrame({"Work_Order_ID": gen_ids("WO", 10), "Work_Order_Number": gen_ids("WN", 10), "Contract_ID": "CTR_01", "Vendor_ID": ven_ids[0], "Asset_ID": "AST_SF", "Request_Date": dates[:10], "Planned_Start_Date": dates[:10], "Planned_End_Date": dates[:10] + pd.Timedelta(days=5), "Actual_End_Date": dates[:10] + pd.Timedelta(days=5), "Work_Order_Status": "Closed", "Approved_Cost": 5000.0})

# AP Invoices flow 5 days after PO, Paid 30 days after
inv_dates = dates + pd.Timedelta(days=5)
due_dates = inv_dates + pd.Timedelta(days=30)
# Status based on today being Mar 10, 2026
statuses = np.where(due_dates <= pd.Timestamp("2026-03-10"), "Paid", "Approved")
fact_invoices = pd.DataFrame({"Invoice_ID": gen_ids("INV", num_days), "Invoice_Number": gen_ids("IN", num_days), "Vendor_ID": ven_ids[0], "Contract_ID": "CTR_01", "PO_ID": fact_purchase_orders["PO_ID"], "Invoice_Date": inv_dates, "Received_Date": inv_dates, "Payment_Terms_Code": "NET30", "Due_Date": due_dates, "Invoice_Amount": po_amts, "Tax_Amount": po_amts * 0.15, "Total_Amount": po_amts * 1.15, "Currency": "SAR", "Invoice_Status": statuses, "Approval_Date": inv_dates + pd.Timedelta(days=1), "Paid_Date": np.where(statuses == "Paid", due_dates, pd.NaT), "Overdue_Days": 0, "Created_TS": pd.to_datetime(inv_dates)})
fact_invoice_lines = pd.DataFrame({"Invoice_Line_ID": gen_ids("INVL", num_days), "Invoice_ID": fact_invoices["Invoice_ID"], "Invoice_Date": fact_invoices["Invoice_Date"], "Item_ID": "ITM_001", "Quantity": 1.0, "Unit_Price": po_amts, "Line_Amount": po_amts})
fact_invoice_rejections = pd.DataFrame([{"Invoice_Rejection_ID": "REJ_01", "Invoice_ID": "INV_001", "Rejected_Date": date(2025,12,7), "Reason_Code": "REJ_01", "Rejected_By": "AP", "Resubmitted_Date": date(2025,12,8)}])
fact_payment_transactions = fact_invoices[fact_invoices["Invoice_Status"] == "Paid"].copy().reset_index()
fact_payment_transactions = pd.DataFrame({"Payment_ID": gen_ids("PAY", len(fact_payment_transactions)), "Invoice_ID": fact_payment_transactions["Invoice_ID"], "Vendor_ID": ven_ids[0], "Payment_Date": fact_payment_transactions["Paid_Date"], "Payment_Amount": fact_payment_transactions["Total_Amount"], "Currency": "SAR", "Payment_Method": "Transfer", "Payment_Status": "Paid", "Payment_Reference": "TRX"})
fact_overdue_payments_daily = pd.DataFrame([{"Date": date(2026,3,10), "Invoice_ID": "INV_090", "Vendor_ID": ven_ids[0], "Due_Date": date(2026,3,1), "Outstanding_Amount": 15000.0, "Overdue_Days": 9, "Aging_Bucket": "0-30", "Status": "Overdue"}])
fact_disputes = pd.DataFrame([{"Dispute_ID": "DSP_01", "Invoice_ID": "INV_002", "Opened_Date": date(2025,12,10), "Closed_Date": date(2025,12,12), "Status": "Resolved", "Dispute_Reason": "Tax Mismatch"}])

# ==========================================
# 7. BIGQUERY LOAD EXECUTION
# ==========================================
tables_to_load = [
    # 1. Generic
    (dim_date, "qic_generic", "dim_date"), (fact_weather_daily, "qic_generic", "fact_weather_daily"), (fact_event_calendar, "qic_generic", "fact_event_calendar"),
    # 2. Shared
    (dim_location, "qic_shared", "dim_location"), (dim_asset, "qic_shared", "dim_asset"), (dim_project, "qic_shared", "dim_project"), (dim_currency, "qic_shared", "dim_currency"),
    # 3. HR
    (dim_department, "qic_hr", "dim_department"), (dim_job, "qic_hr", "dim_job"), (dim_comp_band, "qic_hr", "dim_comp_band"), (dim_employee, "qic_hr", "dim_employee"), (fact_employee_daily_snapshot, "qic_hr", "fact_employee_daily_snapshot"), (fact_payroll_monthly, "qic_hr", "fact_payroll_monthly"), (fact_performance_appraisal, "qic_hr", "fact_performance_appraisal"), (fact_increment_history, "qic_hr", "fact_increment_history"), (fact_absence_daily, "qic_hr", "fact_absence_daily"), (fact_attrition_events, "qic_hr", "fact_attrition_events"),
    # 4. Finance
    (dim_gl_account, "qic_finance", "dim_gl_account"), (dim_cost_center, "qic_finance", "dim_cost_center"), (fact_gl_journal_lines, "qic_finance", "fact_gl_journal_lines"), (fact_budget_monthly, "qic_finance", "fact_budget_monthly"), (fact_forecast_monthly, "qic_finance", "fact_forecast_monthly"), (fact_cashflow_daily, "qic_finance", "fact_cashflow_daily"), (fact_capex_project_monthly, "qic_finance", "fact_capex_project_monthly"),
    # 5. Procurement
    (dim_vendor, "qic_dev_procurement", "dim_vendor"), (dim_item, "qic_dev_procurement", "dim_item"), (fact_contracts, "qic_dev_procurement", "fact_contracts"), (fact_contract_milestones, "qic_dev_procurement", "fact_contract_milestones"), (fact_contract_variations, "qic_dev_procurement", "fact_contract_variations"), (fact_purchase_orders, "qic_dev_procurement", "fact_purchase_orders"), (fact_goods_receipt_lines, "qic_dev_procurement", "fact_goods_receipt_lines"), (fact_work_orders, "qic_dev_procurement", "fact_work_orders"), (fact_tender_pipeline, "qic_dev_procurement", "fact_tender_pipeline"), (fact_vendor_performance_monthly, "qic_dev_procurement", "fact_vendor_performance_monthly"),
    # 6. Payments
    (dim_payment_terms, "qic_dev_payments", "dim_payment_terms"), (dim_rejection_reason, "qic_dev_payments", "dim_rejection_reason"), (fact_invoices, "qic_dev_payments", "fact_invoices"), (fact_invoice_lines, "qic_dev_payments", "fact_invoice_lines"), (fact_invoice_rejections, "qic_dev_payments", "fact_invoice_rejections"), (fact_payment_transactions, "qic_dev_payments", "fact_payment_transactions"), (fact_overdue_payments_daily, "qic_dev_payments", "fact_overdue_payments_daily"), (fact_disputes, "qic_dev_payments", "fact_disputes"),
    # 7. Six Flags
    (dim_gate, "qic_asset_sixflags", "dim_gate"), (dim_ride, "qic_asset_sixflags", "dim_ride"), (dim_outlet, "qic_asset_sixflags", "dim_outlet"), (dim_product, "qic_asset_sixflags", "dim_product"), (dim_ticket_product, "qic_asset_sixflags", "dim_ticket_product"), (fact_ticket_sales, "qic_asset_sixflags", "fact_ticket_sales"), (fact_ticket_redemptions, "qic_asset_sixflags", "fact_ticket_redemptions"), (fact_footfall_gate_hourly, "qic_asset_sixflags", "fact_footfall_gate_hourly"), (fact_ride_operations_hourly, "qic_asset_sixflags", "fact_ride_operations_hourly"), (fact_ride_downtime_events, "qic_asset_sixflags", "fact_ride_downtime_events"), (fact_pos_purchases, "qic_asset_sixflags", "fact_pos_purchases"), (fact_queue_time_samples, "qic_asset_sixflags", "fact_queue_time_samples"), (fact_customer_satisfaction, "qic_asset_sixflags", "fact_customer_satisfaction"), (fact_incidents, "qic_asset_sixflags", "fact_incidents"),
    # 8. AMC
    (dim_cinema, "qic_asset_amc", "dim_cinema"), (dim_film, "qic_asset_amc", "dim_film"), (fact_screenings, "qic_asset_amc", "fact_screenings"), (fact_cinema_ticket_sales, "qic_asset_amc", "fact_cinema_ticket_sales"), (dim_concession_product, "qic_asset_amc", "dim_concession_product"), (fact_fnb_sales, "qic_asset_amc", "fact_fnb_sales"), (fact_occupancy_by_session, "qic_asset_amc", "fact_occupancy_by_session"), (sem_amc_sales_overall_daily, "qic_asset_amc", "sem_amc_sales_overall_daily")
]

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
print(f"\nUploading {len(tables_to_load)} tables to BigQuery...")

for df, ds, tb in tables_to_load:
    try:
        # Dates and Timestamps must be strictly typed for BigQuery
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            if 'Date' in col and 'TS' not in col: df[col] = df[col].dt.date
        client.load_table_from_dataframe(df, f"{PROJECT_ID}.{ds}.{tb}", job_config=job_config).result()
        print(f"✅ Loaded {ds}.{tb} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Failed {ds}.{tb}: {e}")

print("\n🚀 All statistical data successfully injected across all domains!")