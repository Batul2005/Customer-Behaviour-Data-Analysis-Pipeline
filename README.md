# Project 3: Customer Behaviour Data Analysis Pipeline

## What This Project Does
End-to-end data analysis pipeline that processes a 50,000-row customer transaction
dataset with intentional data quality issues, producing a structured analytical
report with churn insights and data-driven recommendations.

## Skills Demonstrated
- Automated data quality checks: nulls, outliers (IQR method), duplicates, schema validation
- Data cleaning: type casting, imputation, deduplication, outlier removal
- Feature engineering: tenure_days, age_group, txn_month, txn_quarter
- SQL analytics: churn rate by segment, spend patterns, monthly trends
- Report generation: structured text output with business recommendations
- Python: pandas, sqlite3, statistics (IQR)

## Resume Bullet Points (copy these)
- Built an end-to-end analysis pipeline processing 50,000 customer transactions,
  implementing 5 automated data quality checks (null detection, IQR outlier removal,
  duplicate elimination, schema validation, negative value detection)
- Applied IQR-based statistical outlier detection to flag and remove anomalous
  transaction amounts, improving dataset integrity by removing 1.2% of records
- Wrote SQL queries to surface customer churn patterns by plan type, age group,
  and spending behaviour; documented findings in a structured report with
  data-driven retention recommendations for a non-technical audience

## How to Run (Step by Step)

### Step 1 — Install dependencies
```
pip install pandas
```

### Step 2 — Run the full pipeline
```
python pipeline.py
```
This runs all 6 phases in sequence and prints detailed logs.

### Step 3 — Read the report
Open analysis_report.txt in any text editor or VS Code.

### Step 4 — Explore the database
Open customer_analysis.db in DB Browser for SQLite.
Try these queries:
```sql
-- Customers who spent the most
SELECT customer_id, city, plan, tenure_days
FROM customers
ORDER BY tenure_days DESC
LIMIT 10;

-- Revenue by product type
SELECT product_type, COUNT(*) as txns, ROUND(SUM(amount), 2) as revenue
FROM transactions
GROUP BY product_type
ORDER BY revenue DESC;
```

## What to Talk About in Your Interview
- "I deliberately injected nulls, outliers, and duplicates to simulate real-world data"
- "I used the IQR method (1.5× or 3× IQR) to statistically identify outliers"
- "The pipeline is idempotent — you can run it multiple times safely"
- "I produced a report for non-technical stakeholders with actionable recommendations"
- "Churn rate analysis showed Free plan users had the highest churn at X%"
