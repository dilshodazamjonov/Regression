import pandas as pd
import json

# ---- Function to safely read JSONL files ----
def read_jsonl_safe(file_path):
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Skipping malformed line {i}: {line[:50]}...")
    return pd.DataFrame(records)

# ---- Function to clean numeric financial columns and save ----
def clean_and_save_financial(df, cols, save_path):

    df = df.rename(columns={'cust_num': 'customer_id'})
    
    # ---- Clean numeric columns ----
    for col in cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r'[\$,]', '', regex=True)  # remove $ and ,
            .str.strip()
            .replace('', pd.NA)  # empty strings to NA
            .pipe(pd.to_numeric, errors='coerce')
        )
    
    # ---- Round specific columns to 2 decimal places ----
    round_cols = ['debt_service_ratio', 'total_monthly_debt_payment',
                  'annual_debt_payment', 'loan_to_annual_income',
                  'monthly_free_cash_flow']
    for col in round_cols:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    df.to_csv(save_path, index=False)
    return df


# ---- Columns to convert to INT ----
categorical_cols = [
    'monthly_income', 'existing_monthly_debt', 'monthly_payment',
    'revolving_balance', 'credit_usage_amount', 'available_credit',
    'total_monthly_debt_payment', 'total_debt_amount', 'monthly_free_cash_flow'
]

financial_df = read_jsonl_safe('FinancialModel/data/financial_ratios.jsonl')

financial_df = clean_and_save_financial(
    financial_df,
    categorical_cols,
    'FinancialModel/data/sending/financial_cleaned.csv'
)

print(financial_df.head())
