import pandas as pd
import numpy as np
from datetime import date
from google.cloud import bigquery

# ==========================================
# 1. SETUP
# ==========================================
PROJECT_ID = "smartadvisor-483817" # <--- Update this
client = bigquery.Client(project=PROJECT_ID)

# Datasets
DS_HR = f"{PROJECT_ID}.qic_hr"
DS_PAY = f"{PROJECT_ID}.qic_dev_payments"

# Dates for our target analytical window
months = pd.date_range("2025-12-01", "2026-03-01", freq='MS').date
daily_dates = pd.date_range("2025-12-01", "2026-03-10").date
np.random.seed(42)

print("--- 1. Generating HR Data Mart (dm_employee_monthly_360) ---")

# Generate Base Employees (Dimension embedded in Fact)
num_emps = 50
emp_ids = [f"EMP_{str(i).zfill(3)}" for i in range(1, num_emps + 1)]
depts = ["Operations", "Human Resources", "Finance", "IT"]
jobs = ["Manager", "Specialist", "Coordinator"]

hr_rows = []
for m in months:
    for emp in emp_ids:
        # Static attributes per employee (hashed by ID to stay consistent across months)
        emp_hash = hash(emp)
        dept = depts[emp_hash % len(depts)]
        job = jobs[emp_hash % len(jobs)]
        is_saudi = bool(emp_hash % 2 == 0)
        base_salary = 10000.0 + (emp_hash % 5000)
        
        # Dynamic monthly attributes (Facts)
        absences = np.random.choice([0, 0, 0, 8, 16], p=[0.7, 0.1, 0.1, 0.05, 0.05])
        perf_score = np.random.normal(3.5, 0.5) if m.month == 12 else None # Annual review in Dec
        
        hr_rows.append({
            "Snapshot_Month": m,
            "Employee_ID": emp,
            "Is_Saudi": is_saudi,
            "Employment_Status": "Active",
            "Department_Name": dept,
            "Business_Unit": "Corporate" if dept in ["Finance", "HR", "IT"] else "Parks",
            "Job_Title": job,
            "Job_Level": "L3" if job == "Manager" else "L1",
            "Base_Salary_Monthly": base_salary,
            "Gross_Pay_Monthly": base_salary + 1500.0, # Adding standard allowance
            "Hours_Absent_This_Month": absences,
            "Performance_Score_YTD": round(perf_score, 1) if perf_score else None
        })

dm_hr = pd.DataFrame(hr_rows)

print("--- 2. Generating AP Data Mart (dm_invoice_lifecycle_360) ---")

inv_rows = []
for i, d in enumerate(daily_dates):
    # Create 1-3 invoices per day
    num_invoices = np.random.randint(1, 4)
    for j in range(num_voices := num_invoices):
        inv_id = f"INV_{d.strftime('%Y%m%d')}_{j}"
        amt = np.random.uniform(5000, 50000)
        
        # Payment Terms Logic
        terms = np.random.choice(["NET30", "NET60"], p=[0.8, 0.2])
        term_days = 30 if terms == "NET30" else 60
        due_date = d + pd.Timedelta(days=term_days)
        
        # Dispute/Rejection Logic (Statistical anomalies)
        is_rejected = np.random.choice([True, False], p=[0.05, 0.95])
        rejection_reason = "Missing PO Match" if is_rejected else None
        
        is_disputed = np.random.choice([True, False], p=[0.02, 0.98])
        
        # Payment Status based on cutoff date (Mar 10, 2026)
        is_paid = due_date <= date(2026, 3, 10) and not is_disputed
        
        inv_rows.append({
            "Invoice_ID": inv_id,
            "Invoice_Date": d,
            "Vendor_ID": f"VEN_{np.random.randint(1,10)}",
            "PO_ID": f"PO_{inv_id}",
            "Payment_Terms_Code": terms,
            "Terms_Days": term_days,
            "Due_Date": due_date,
            "Invoice_Amount_Net": round(amt, 2),
            "Tax_Amount": round(amt * 0.15, 2),
            "Total_Amount_Gross": round(amt * 1.15, 2),
            "Has_Been_Rejected": is_rejected,
            "Latest_Rejection_Reason": rejection_reason,
            "Has_Active_Dispute": is_disputed,
            "Payment_Status": "Paid" if is_paid else ("Overdue" if due_date < date(2026,3,10) else "Pending"),
            "Paid_Date": due_date if is_paid else None,
            "Overdue_Days": max(0, (date(2026,3,10) - due_date).days) if not is_paid else 0
        })

dm_ap = pd.DataFrame(inv_rows)


# ==========================================
# 3. SCHEMA DEFINITIONS WITH METADATA
# ==========================================
print("--- 3. Defining Schemas and Pushing to BigQuery ---")

hr_schema = [
    bigquery.SchemaField("Snapshot_Month", "DATE", description="First day of the reporting month. Used for time-series aggregation."),
    bigquery.SchemaField("Employee_ID", "STRING", description="Unique identifier for the employee."),
    bigquery.SchemaField("Is_Saudi", "BOOLEAN", description="True if Saudi National (used for Saudization metrics)."),
    bigquery.SchemaField("Employment_Status", "STRING", description="Active, OnLeave, or Terminated."),
    bigquery.SchemaField("Department_Name", "STRING", description="Denormalized department name (e.g., Operations, Finance)."),
    bigquery.SchemaField("Business_Unit", "STRING", description="Higher-level grouping of departments."),
    bigquery.SchemaField("Job_Title", "STRING", description="Employee's current job title."),
    bigquery.SchemaField("Job_Level", "STRING", description="Seniority level (L1, L2, L3)."),
    bigquery.SchemaField("Base_Salary_Monthly", "FLOAT", description="Base monthly salary in SAR."),
    bigquery.SchemaField("Gross_Pay_Monthly", "FLOAT", description="Total pay including allowances before deductions."),
    bigquery.SchemaField("Hours_Absent_This_Month", "INTEGER", description="Total hours absent (sick/annual) in this specific month."),
    bigquery.SchemaField("Performance_Score_YTD", "FLOAT", description="Latest performance score (1.0 to 5.0). Null if unrated.")
]

ap_schema = [
    bigquery.SchemaField("Invoice_ID", "STRING", description="Primary Key for the invoice."),
    bigquery.SchemaField("Invoice_Date", "DATE", description="Date the invoice was issued."),
    bigquery.SchemaField("Vendor_ID", "STRING", description="Vendor identifier."),
    bigquery.SchemaField("PO_ID", "STRING", description="Linked Purchase Order."),
    bigquery.SchemaField("Payment_Terms_Code", "STRING", description="Payment terms (e.g., NET30, NET60)."),
    bigquery.SchemaField("Terms_Days", "INTEGER", description="Numeric representation of terms (30, 60)."),
    bigquery.SchemaField("Due_Date", "DATE", description="Calculated due date (Invoice Date + Terms Days)."),
    bigquery.SchemaField("Invoice_Amount_Net", "FLOAT", description="Amount before VAT."),
    bigquery.SchemaField("Tax_Amount", "FLOAT", description="15% VAT."),
    bigquery.SchemaField("Total_Amount_Gross", "FLOAT", description="Total amount payable."),
    bigquery.SchemaField("Has_Been_Rejected", "BOOLEAN", description="True if invoice was ever rejected by AP."),
    bigquery.SchemaField("Latest_Rejection_Reason", "STRING", description="Reason text if rejected (e.g., Missing PO Match)."),
    bigquery.SchemaField("Has_Active_Dispute", "BOOLEAN", description="True if currently under dispute."),
    bigquery.SchemaField("Payment_Status", "STRING", description="Paid, Pending, or Overdue based on current date."),
    bigquery.SchemaField("Paid_Date", "DATE", description="Date the payment was cleared. Null if unpaid."),
    bigquery.SchemaField("Overdue_Days", "INTEGER", description="Days past due date. 0 if paid or pending.")
]

def load_datamart(df, table_id, schema, description):
    # Set up job config to apply the schema and truncate old data
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    
    # Load the data
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    # Apply table-level description
    table = client.get_table(table_id)
    table.description = description
    client.update_table(table, ["description"])
    
    print(f"✅ Created Datamart: {table_id} ({len(df)} rows)")

# Execute
load_datamart(
    df=dm_hr, 
    table_id=f"{DS_HR}.dm_employee_monthly_360", 
    schema=hr_schema, 
    description="Flattened HR Data Mart. Grain: 1 row per Employee per Month. Avoids joining DIM_DEPARTMENT, DIM_JOB, and FACT_PAYROLL."
)

load_datamart(
    df=dm_ap, 
    table_id=f"{DS_PAY}.dm_invoice_lifecycle_360", 
    schema=ap_schema, 
    description="Flattened AP Data Mart. Grain: 1 row per Invoice. Avoids joining DIM_PAYMENT_TERMS, FACT_REJECTIONS, and FACT_PAYMENTS."
)

print("\n🚀 Datamarts successfully pushed with full metadata!")