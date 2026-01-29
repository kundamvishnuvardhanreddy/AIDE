import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt

# ------------------------------
# Sample Data 
# ------------------------------
data = {
    "customer_id": [1, 2, 3, 4, 5],
    "email": ["a@test.com", "b@test.com", None, "invalid_email", "e@test.com"],
    "age": [25, 120, 35, -5, 40],
    "salary": [50000, 52000, 51000, 1000000, 53000]
}

df = pd.DataFrame(data)

# ------------------------------
# Data Profiling
# ------------------------------
profile = {}
for col in df.columns:
    profile[col] = {
        "dtype": str(df[col].dtype),
        "nulls": int(df[col].isna().sum()),
        "unique": int(df[col].nunique())
    }
    if pd.api.types.is_numeric_dtype(df[col]):
        profile[col].update({
            "min": float(df[col].min()),
            "max": float(df[col].max()),
            "mean": float(df[col].mean())
        })

# ------------------------------
# Rule-Based Data Quality Checks
# ------------------------------
issues = []

if "age" in df.columns:
    bad_age = df[(df["age"] < 0) | (df["age"] > 100)]
    if not bad_age.empty:
        issues.append({"rule": "Invalid age", "rows": bad_age.index.tolist()})

if "email" in df.columns:
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    bad_email = df[~df["email"].fillna("").str.match(email_regex)]
    if not bad_email.empty:
        issues.append({"rule": "Invalid email", "rows": bad_email.index.tolist()})

# ------------------------------
# AI Anomaly Detection
# ------------------------------
num_df = df.select_dtypes(include=np.number).fillna(0)
model = IsolationForest(contamination=0.1, random_state=42)
df["anomaly"] = model.fit_predict(num_df) == -1

# ------------------------------
# Schema Drift Detection
# ------------------------------
reference_schema = {
    "customer_id": "int64",
    "email": "object",
    "age": "int64",
    "salary": "int64"
}

schema_drift = []
for col, dtype in df.dtypes.items():
    if col == "anomaly": continue
    if col not in reference_schema:
        schema_drift.append(f"New column: {col}")
    elif reference_schema[col] != str(dtype):
        schema_drift.append(f"Type change: {col}")

for col in reference_schema:
    if col not in df.columns:
        schema_drift.append(f"Missing column: {col}")

# ------------------------------
# PII Detection
# ------------------------------
pii = []
email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"

for col in df.columns:
    for val in df[col].dropna().astype(str).head(10):
        if re.search(email_pattern, val):
            pii.append({"column": col, "type": "EMAIL"})
            break

# ------------------------------
# Data Quality Score
# ------------------------------
total_cells = df.size
penalty = df.isna().sum().sum() + sum(len(i["rows"]) for i in issues)
quality_score = round(max(0, 100 - (penalty / total_cells) * 100), 2)

# ------------------------------
# Governance Audit Log
# ------------------------------
audit_log = {
    "timestamp": str(datetime.utcnow()),
    "quality_score": quality_score,
    "issues": issues,
    "anomalies": int(df["anomaly"].sum()),
    "pii_detected": pii,
    "schema_drift": schema_drift
}

with open("governance_log.json", "w") as f:
    json.dump(audit_log, f, indent=2)

# ------------------------------
# VISUALIZATION
# ------------------------------
def plot_governance():
    print("Generating chart...")
    plt.figure(figsize=(10, 6))
    
    # Data for the bar chart
    metrics = ['Quality Score (%)', 'Rule Issues', 'AI Anomalies']
    values = [quality_score, len(issues), int(df["anomaly"].sum())]
    
    colors = ['#4CAF50', '#FF9800', '#f44336'] # Green, Orange, Red
    
    bars = plt.bar(metrics, values, color=colors)
    plt.title("AI Data Governance & Quality Report", fontsize=14)
    plt.ylabel("Value / Count")
    
    # Add data labels on top of bars
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 1, yval, ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    plt.show()

# ------------------------------
# Output Summary
# ------------------------------
print("\n" + "="*30)
print(" AI Data Governance Summary")
print("="*30)
print(f"Quality Score:  {quality_score}%")
print(f"Rules Broken:   {len(issues)}")
print(f"AI Anomalies:   {int(df['anomaly'].sum())}")
print(f"PII Detected:   {[p['column'] for p in pii]}")
print(f"Schema Drift:   {schema_drift if schema_drift else 'None'}")
print("-" * 30)
print("Audit log saved as: governance_log.json")

# Show the graph
plot_governance()

# Final pause for VS Code Terminal
input("\nExecution complete. Press Enter to exit...")