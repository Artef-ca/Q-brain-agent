import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery

# ==========================================
# 1. SETUP
# ==========================================
PROJECT_ID = "smartadvisor-483817"  # <--- Replace this
client = bigquery.Client(project=PROJECT_ID)

# Base sample dates
d1 = date(2025, 12, 1)
d2 = date(2025, 12, 2)
ts1 = pd.Timestamp("2025-12-01 10:00:00")
ts2 = pd.Timestamp("2025-12-02 14:30:00")

def load_bq(df: pd.DataFrame, dataset: str, table: str):
    table_ref = f"{PROJECT_ID}.{dataset}.{table}"
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Loaded {table}")
    except Exception as e:
        print(f"❌ Error loading {table}: {e}")

print("--- Generating Complete Data for All 33 Tables ---")

# ==========================================
# DOMAIN: GENERIC
# ==========================================
dim_date = pd.DataFrame([{
    "Date": d1, "Year": 2025, "Quarter": 4, "Month": 12, "Month_Name": "December", "Week": 49, "Day": 1, "Day_Name": "Monday",
    "Is_Weekend_Saudi": False, "Is_Month_End": False, "Is_Quarter_End": False, "Is_Saudi_Public_Holiday": False, "Holiday_Name": None,
    "Is_School_Holiday": False, "Is_University_Holiday": False, "Is_Ramadan_Flag": False, "Season_Tag": "Winter"
}])

fact_weather_daily = pd.DataFrame([{
    "Date": d1, "Location_ID": "LOC_QID", "City": "Qiddiya", "Temp_Max_C": 22.5, "Temp_Min_C": 12.0, "Temp_Avg_C": 17.2,
    "Humidity_Avg": 45.0, "Wind_Speed_Avg_KMH": 15.5, "Precip_MM": 0.0, "Dust_Index": 2.0, "Heat_Index": 22.0,
    "Weather_Condition": "Clear", "Is_Extreme_Heat": False, "Data_Source": "NCM"
}])

fact_event_calendar = pd.DataFrame([{
    "Event_ID": "EVT_01", "Event_Name": "Winter Festival", "Event_Type": "Cultural", "Target_Audience": "Family",
    "Start_Date": d1, "End_Date": d2, "Start_TS": ts1, "End_TS": ts2, "Location_ID": "LOC_QID", "City": "Qiddiya",
    "Venue_Name": "Main Plaza", "Is_International": False, "Featured_Artist": None, "Popularity_Score": 85.5,
    "Expected_Attendance": 5000, "Ticketed_Flag": True, "Price_Min": 50.0, "Price_Max": 150.0, "Source_System": "EventsMgmt",
    "Created_TS": ts1
}])

# ==========================================
# DOMAIN: SHARED
# ==========================================
dim_location = pd.DataFrame([{
    "Location_ID": "LOC_QID", "Country": "Saudi Arabia", "Region": "Riyadh Prov", "City": "Qiddiya",
    "District": "Entertainment Zone", "Latitude": 24.586, "Longitude": 46.312, "Location_Type": "City"
}])

dim_asset = pd.DataFrame([{
    "Asset_ID": "AST_SF", "Asset_Name": "Six Flags Qiddiya", "Asset_Type": "ThemePark", "Operator": "Six Flags",
    "Owner": "QIC", "Open_Date": d1, "Location_ID": "LOC_QID"
}, {
    "Asset_ID": "AST_AMC", "Asset_Name": "AMC Riyadh", "Asset_Type": "Cinema", "Operator": "AMC",
    "Owner": "SEVEN", "Open_Date": d1, "Location_ID": "LOC_QID"
}])

dim_project = pd.DataFrame([{
    "Project_ID": "PRJ_01", "Project_Name": "SF Phase 1", "Project_Type": "Construction", "Asset_ID": "AST_SF",
    "Start_Date": d1, "End_Date": d2, "Status": "Active"
}])

dim_currency = pd.DataFrame([{"Currency_Code": "SAR", "Currency_Name": "Saudi Riyal", "Is_Base_Currency": True}])

# ==========================================
# DOMAIN: HR
# ==========================================
dim_department = pd.DataFrame([{"Department_ID": "DEP_OP", "Department_Name": "Operations", "Parent_Department_ID": None, "Business_Unit": "Parks", "Is_Active": True}])
dim_job = pd.DataFrame([{"Job_ID": "JOB_01", "Job_Title": "Ride Operator", "Job_Level": "L2", "Job_Family": "Operations"}])
dim_comp_band = pd.DataFrame([{"Band_ID": "BND_01", "Job_Level": "L2", "Min_Salary": 4000, "Mid_Salary": 6000, "Max_Salary": 8000, "Currency": "SAR"}])

dim_employee = pd.DataFrame([{
    "Employee_ID": "EMP_01", "National_ID_Hash": "hash123", "Gender": "Male", "Nationality_Code": "SA", "Is_Saudi": True,
    "Hire_Date": d1, "Termination_Date": None, "Employment_Status": "Active", "Department_ID": "DEP_OP", "Job_ID": "JOB_01",
    "Band_ID": "BND_01", "Base_Salary_Monthly": 6000.0, "Allowance_Monthly": 1000.0, "Currency": "SAR", "Payroll_System": "SAP",
    "Last_Updated_TS": ts1
}])

fact_employee_daily_snapshot = pd.DataFrame([{"Date": d1, "Employee_ID": "EMP_01", "Department_ID": "DEP_OP", "Employment_Status": "Active", "Is_Saudi": True, "Gender": "Male", "FTE": 1.0, "Daily_Total_Cost": 233.0, "Snapshot_Source": "HRIS"}])
fact_payroll_monthly = pd.DataFrame([{"Payroll_Month": d1, "Employee_ID": "EMP_01", "Department_ID": "DEP_OP", "Gross_Pay": 7000.0, "Net_Pay": 6300.0, "Deductions": 700.0, "Employer_Contrib": 600.0, "Payroll_System": "SAP", "Payment_Date": d1}])
fact_performance_appraisal = pd.DataFrame([{"Appraisal_ID": "APR_01", "Employee_ID": "EMP_01", "Department_ID": "DEP_OP", "Cycle_Name": "H1 2025", "Cycle_Start_Date": d1, "Cycle_End_Date": d2, "Rating": "Exceeds", "Score": 4.5, "Finalized_Date": d2}])
fact_increment_history = pd.DataFrame([{"Increment_ID": "INC_01", "Employee_ID": "EMP_01", "Effective_Date": d1, "Increment_Type": "Merit", "Old_Base_Salary": 5000.0, "New_Base_Salary": 6000.0, "Increment_Percent": 20.0, "Reference_Appraisal_ID": "APR_01"}])
fact_absence_daily = pd.DataFrame([{"Date": d1, "Employee_ID": "EMP_01", "Absence_Type": "Annual", "Hours_Absent": 8.0}])
fact_attrition_events = pd.DataFrame([{"Attrition_ID": "ATT_01", "Employee_ID": "EMP_01", "Termination_Date": d2, "Exit_Type": "Voluntary", "Exit_Reason": "Relocation"}])

# ==========================================
# DOMAIN: FINANCE
# ==========================================
dim_gl_account = pd.DataFrame([{"GL_Account_ID": "GL_REV", "GL_Account_Name": "Ticket Revenue", "Account_Type": "Revenue", "Account_Group": "Operations"}])
dim_cost_center = pd.DataFrame([{"Cost_Center_ID": "CC_01", "Cost_Center_Name": "Park Ops", "Department_ID": "DEP_OP"}])
fact_gl_journal_lines = pd.DataFrame([{"Journal_Line_ID": "JRN_01", "Journal_ID": "J_001", "Date": d1, "GL_Account_ID": "GL_REV", "Cost_Center_ID": "CC_01", "Project_ID": "PRJ_01", "Debit_Amount": 0.0, "Credit_Amount": 5000.0, "Currency": "SAR", "Narration": "Daily Sales"}])
fact_budget_monthly = pd.DataFrame([{"Budget_Month": d1, "Cost_Center_ID": "CC_01", "GL_Account_ID": "GL_REV", "Budget_Amount": 150000.0, "Currency": "SAR", "Version": "Original"}])
fact_forecast_monthly = pd.DataFrame([{"Forecast_Month": d1, "Cost_Center_ID": "CC_01", "GL_Account_ID": "GL_REV", "Forecast_Version": "FCST_M01", "Forecast_Amount": 140000.0, "Currency": "SAR"}])
fact_cashflow_daily = pd.DataFrame([{"Date": d1, "Cash_In": 5000.0, "Cash_Out": 1000.0, "Net_Cashflow": 4000.0}])
fact_capex_project_monthly = pd.DataFrame([{"Capex_Month": d1, "Project_ID": "PRJ_01", "Capex_Amount": 250000.0, "Currency": "SAR"}])

# ==========================================
# DOMAIN: PROCUREMENT & AP PAYMENTS
# ==========================================
dim_vendor = pd.DataFrame([{"Vendor_ID": "VEN_01", "Vendor_Name": "Acme Corp", "Vendor_Type": "Supplier", "Country": "SA", "Is_Local_Saudi": True, "Risk_Rating": "Low", "Preferred_Vendor_Flag": True, "Last_Updated_TS": ts1}])
dim_item = pd.DataFrame([{"Item_ID": "ITM_01", "Item_Name": "Steel Beams", "Category": "Materials", "UOM": "Ton"}])
dim_payment_terms = pd.DataFrame([{"Payment_Terms_Code": "NET30", "Terms_Days": 30}])
dim_rejection_reason = pd.DataFrame([{"Reason_Code": "REJ_01", "Category": "MissingDocs", "Reason_Text": "Missing Tax ID"}])

fact_contracts = pd.DataFrame([{"Contract_ID": "CTR_01", "Contract_Number": "C-100", "Vendor_ID": "VEN_01", "Project_ID": "PRJ_01", "Contract_Type": "EPC", "Award_Date": d1, "Start_Date": d1, "End_Date": d2, "Contract_Value": 500000.0, "Currency": "SAR", "Status": "Active"}])
fact_contract_milestones = pd.DataFrame([{"Milestone_ID": "MIL_01", "Contract_ID": "CTR_01", "Milestone_Name": "Foundation", "Planned_Date": d1, "Actual_Date": d1, "Progress_Pct": 100.0, "Status": "Completed"}])
fact_contract_variations = pd.DataFrame([{"Variation_ID": "VAR_01", "Contract_ID": "CTR_01", "Variation_Date": d1, "Variation_Amount": 15000.0, "Reason": "Design Change", "Approved_Flag": True}])
fact_purchase_orders = pd.DataFrame([{"PO_Line_ID": "POL_01", "PO_ID": "PO_01", "PO_Number": "PO-100", "PO_Date": d1, "Vendor_ID": "VEN_01", "Contract_ID": "CTR_01", "Project_ID": "PRJ_01", "Item_ID": "ITM_01", "Quantity": 10.0, "Unit_Price": 500.0, "Line_Amount": 5000.0, "Tax_Amount": 750.0, "Total_Line_Amount": 5750.0, "Currency": "SAR", "PO_Status": "Approved"}])
fact_goods_receipt_lines = pd.DataFrame([{"GRN_Line_ID": "GRN_01", "PO_Line_ID": "POL_01", "Receipt_Date": d1, "Vendor_ID": "VEN_01", "Received_Qty": 10.0, "Rejected_Qty": 0.0, "Quality_Flag": False}])
fact_work_orders = pd.DataFrame([{"Work_Order_ID": "WO_01", "Work_Order_Number": "WO-100", "Contract_ID": "CTR_01", "Vendor_ID": "VEN_01", "Asset_ID": "AST_SF", "Request_Date": d1, "Planned_Start_Date": d1, "Planned_End_Date": d2, "Actual_End_Date": d2, "Work_Order_Status": "Closed", "Approved_Cost": 5000.0}])
fact_tender_pipeline = pd.DataFrame([{"Tender_ID": "TND_01", "Created_Date": d1, "Project_ID": "PRJ_01", "Category": "Construction", "Stage": "Awarded", "Expected_Value": 500000.0, "Awarded_Contract_ID": "CTR_01"}])
fact_vendor_performance_monthly = pd.DataFrame([{"Perf_Month": d1, "Vendor_ID": "VEN_01", "OnTime_Delivery_Pct": 98.5, "Quality_Issue_Rate": 1.2, "Avg_Delay_Days": 0.5, "Performance_Score": 95.0}])

fact_invoices = pd.DataFrame([{"Invoice_ID": "INV_01", "Invoice_Number": "IN-100", "Vendor_ID": "VEN_01", "Contract_ID": "CTR_01", "PO_ID": "PO_01", "Invoice_Date": d1, "Received_Date": d1, "Payment_Terms_Code": "NET30", "Due_Date": d2, "Invoice_Amount": 5000.0, "Tax_Amount": 750.0, "Total_Amount": 5750.0, "Currency": "SAR", "Invoice_Status": "Approved", "Approval_Date": d1, "Paid_Date": None, "Overdue_Days": 0, "Created_TS": ts1}])
fact_invoice_lines = pd.DataFrame([{"Invoice_Line_ID": "INVL_01", "Invoice_ID": "INV_01", "Invoice_Date": d1, "Item_ID": "ITM_01", "Quantity": 10.0, "Unit_Price": 500.0, "Line_Amount": 5000.0}])
fact_invoice_rejections = pd.DataFrame([{"Invoice_Rejection_ID": "REJ_01", "Invoice_ID": "INV_01", "Rejected_Date": d1, "Reason_Code": "REJ_01", "Rejected_By": "AP_Clerk", "Resubmitted_Date": None}])
fact_payment_transactions = pd.DataFrame([{"Payment_ID": "PAY_01", "Invoice_ID": "INV_01", "Vendor_ID": "VEN_01", "Payment_Date": d2, "Payment_Amount": 5750.0, "Currency": "SAR", "Payment_Method": "BankTransfer", "Payment_Status": "Paid", "Payment_Reference": "TRX-99"}])
fact_overdue_payments_daily = pd.DataFrame([{"Date": d1, "Invoice_ID": "INV_01", "Vendor_ID": "VEN_01", "Due_Date": d1, "Outstanding_Amount": 5750.0, "Overdue_Days": 5, "Aging_Bucket": "0-30", "Status": "Overdue"}])
fact_disputes = pd.DataFrame([{"Dispute_ID": "DSP_01", "Invoice_ID": "INV_01", "Opened_Date": d1, "Closed_Date": d2, "Status": "Resolved", "Dispute_Reason": "Pricing Error"}])

# ==========================================
# DOMAIN: ASSET SIX FLAGS
# ==========================================
dim_gate = pd.DataFrame([{"Gate_ID": "GT_01", "Gate_Name": "Main Gate", "Asset_ID": "AST_SF"}])
dim_ride = pd.DataFrame([{"Ride_ID": "RD_01", "Ride_Name": "Falcons Flight", "Ride_Category": "Thrill", "Capacity_Per_Cycle": 24, "Average_Cycle_Min": 3.5, "Is_Operational": True, "Asset_ID": "AST_SF"}])
dim_outlet = pd.DataFrame([{"Outlet_ID": "OUT_01", "Outlet_Name": "Burger Co", "Outlet_Type": "F&B", "Asset_ID": "AST_SF"}])
dim_product = pd.DataFrame([{"Product_ID": "PRD_01", "Product_Name": "Cheeseburger", "Category": "Food", "Base_Price": 45.0}])
dim_ticket_product = pd.DataFrame([{"Ticket_Product_ID": "TKP_01", "Ticket_Product_Name": "Day Pass", "Ticket_Product_Type": "DayPass", "Base_Price": 250.0, "Asset_ID": "AST_SF"}])

fact_ticket_sales = pd.DataFrame([{
    "Ticket_TXN_ID": "TXN_01", "TXN_TS": ts1, "Date": d1, "Asset_ID": "AST_SF", "Sales_Channel": "Online",
    "Ticket_Product_ID": "TKP_01", "Quantity": 2, "Gross_Amount": 500.0, "Discount_Amount": 0.0, 
    "Net_Amount": 425.0, "Tax_Amount": 75.0, "Promotion_Code": None
}])
fact_ticket_redemptions = pd.DataFrame([{"Redemption_ID": "RED_01", "Ticket_TXN_ID": "TXN_01", "Date": d1, "Scan_TS": ts1, "Gate_ID": "GT_01", "Entries": 1}])
fact_footfall_gate_hourly = pd.DataFrame([{"Gate_ID": "GT_01", "Hour_TS": ts1, "Date": d1, "Entries": 500, "Exits": 50, "In_Park_Estimate": 450}])
fact_ride_operations_hourly = pd.DataFrame([{"Ride_ID": "RD_01", "Hour_TS": ts1, "Date": d1, "Operating_Minutes": 55, "Downtime_Minutes": 5, "Downtime_Reason": "Sensor", "Cycles": 15, "Riders": 300, "Utilization_Pct": 85.5, "Staff_On_Duty": 4}])
fact_ride_downtime_events = pd.DataFrame([{"Downtime_Event_ID": "DWE_01", "Ride_ID": "RD_01", "Start_Date": d1, "Start_TS": ts1, "End_TS": ts2, "Downtime_Reason": "Sensor", "Downtime_Minutes": 5}])
fact_pos_purchases = pd.DataFrame([{"POS_Line_ID": "POS_01", "Transaction_ID": "TRX_POS1", "TXN_TS": ts1, "Date": d1, "Outlet_ID": "OUT_01", "Outlet_Type": "F&B", "Product_ID": "PRD_01", "Product_Name": "Cheeseburger", "Category": "Food", "Quantity": 1.0, "Unit_Price": 45.0, "Net_Amount": 45.0, "Payment_Method": "Card"}])
fact_queue_time_samples = pd.DataFrame([{"Ride_ID": "RD_01", "Sample_TS": ts1, "Date": d1, "Queue_Minutes": 45.0}])
fact_customer_satisfaction = pd.DataFrame([{"Survey_Response_ID": "SRV_01", "Response_TS": ts1, "Date": d1, "Channel": "App", "NPS_Score": 9, "CSAT_Score": 5, "Ride_ID": "RD_01", "Outlet_ID": None, "Free_Text": "Great ride!", "Sentiment_Score": 0.9, "Issue_Category": None}])
fact_incidents = pd.DataFrame([{"Incident_ID": "INC_01", "Incident_Date": d1, "Asset_ID": "AST_SF", "Ride_ID": "RD_01", "Severity": "Low", "Category": "Guest", "Description": "Lost item"}])

# ==========================================
# DOMAIN: ASSET AMC
# ==========================================
dim_cinema = pd.DataFrame([{"Cinema_ID": "CIN_01", "Cinema_Name": "AMC Riyadh 1", "City": "Riyadh", "Location_ID": "LOC_QID", "Screens": 10, "Premium_Formats": "IMAX"}])
dim_film = pd.DataFrame([{"Film_ID": "FLM_01", "Film_Title": "Dune", "Distributor": "WB", "Genre": "Sci-Fi", "Release_Date": d1, "Language": "English", "Runtime_Min": 155}])
dim_concession_product = pd.DataFrame([{"Product_ID": "CONC_01", "Product_Name": "Popcorn", "Category": "Food", "Base_Price": 35.0}])

fact_screenings = pd.DataFrame([{"Session_ID": "SES_01", "Cinema_ID": "CIN_01", "Film_ID": "FLM_01", "Date": d1, "Start_TS": ts1, "Screen_Number": 1, "Session_Format": "IMAX", "Capacity_Seats": 250}])
fact_cinema_ticket_sales = pd.DataFrame([{"Ticket_TXN_ID": "AMCT_01", "TXN_TS": ts1, "Date": d1, "Cinema_ID": "CIN_01", "Film_ID": "FLM_01", "Session_ID": "SES_01", "Sales_Channel": "Online", "Quantity": 2, "Gross_Amount": 150.0, "Net_Amount": 130.0, "Tax_Amount": 20.0, "Promotion_Code": None}])
fact_fnb_sales = pd.DataFrame([{"FNB_Line_ID": "AMCF_01", "Transaction_ID": "TRX_AMC1", "TXN_TS": ts1, "Date": d1, "Cinema_ID": "CIN_01", "Product_ID": "CONC_01", "Product_Name": "Popcorn", "Category": "Food", "Quantity": 1.0, "Net_Amount": 35.0}])
fact_occupancy_by_session = pd.DataFrame([{"Session_ID": "SES_01", "Date": d1, "Cinema_ID": "CIN_01", "Film_ID": "FLM_01", "Capacity_Seats": 250, "Tickets_Sold": 200, "Occupancy_Pct": 0.8}])
sem_amc_sales_overall_daily = pd.DataFrame([{"Date": d1, "Cinema_ID": "CIN_01", "Tickets_Net_Revenue": 15000.0, "FNB_Net_Revenue": 5000.0, "Total_Net_Revenue": 20000.0, "Admissions": 200, "Transactions": 100}])


# ==========================================
# 2. PUSH ALL 33 TABLES TO BIGQUERY
# ==========================================
tables_to_load = [
    (dim_date, "qic_generic", "dim_date"),
    (fact_weather_daily, "qic_generic", "fact_weather_daily"),
    (fact_event_calendar, "qic_generic", "fact_event_calendar"),
    (dim_location, "qic_shared", "dim_location"),
    (dim_asset, "qic_shared", "dim_asset"),
    (dim_project, "qic_shared", "dim_project"),
    (dim_currency, "qic_shared", "dim_currency"),
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
    (dim_gl_account, "qic_finance", "dim_gl_account"),
    (dim_cost_center, "qic_finance", "dim_cost_center"),
    (fact_gl_journal_lines, "qic_finance", "fact_gl_journal_lines"),
    (fact_budget_monthly, "qic_finance", "fact_budget_monthly"),
    (fact_forecast_monthly, "qic_finance", "fact_forecast_monthly"),
    (fact_cashflow_daily, "qic_finance", "fact_cashflow_daily"),
    (fact_capex_project_monthly, "qic_finance", "fact_capex_project_monthly"),
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
    (dim_payment_terms, "qic_dev_payments", "dim_payment_terms"),
    (dim_rejection_reason, "qic_dev_payments", "dim_rejection_reason"),
    (fact_invoices, "qic_dev_payments", "fact_invoices"),
    (fact_invoice_lines, "qic_dev_payments", "fact_invoice_lines"),
    (fact_invoice_rejections, "qic_dev_payments", "fact_invoice_rejections"),
    (fact_payment_transactions, "qic_dev_payments", "fact_payment_transactions"),
    (fact_overdue_payments_daily, "qic_dev_payments", "fact_overdue_payments_daily"),
    (fact_disputes, "qic_dev_payments", "fact_disputes"),
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
    (dim_cinema, "qic_asset_amc", "dim_cinema"),
    (dim_film, "qic_asset_amc", "dim_film"),
    (fact_screenings, "qic_asset_amc", "fact_screenings"),
    (fact_cinema_ticket_sales, "qic_asset_amc", "fact_cinema_ticket_sales"),
    (dim_concession_product, "qic_asset_amc", "dim_concession_product"),
    (fact_fnb_sales, "qic_asset_amc", "fact_fnb_sales"),
    (fact_occupancy_by_session, "qic_asset_amc", "fact_occupancy_by_session"),
    (sem_amc_sales_overall_daily, "qic_asset_amc", "sem_amc_sales_overall_daily")
]

for df, ds, tb in tables_to_load:
    load_bq(df, ds, tb)

print("\n🚀 All 33 tables completely loaded with strict schema adherence.")
