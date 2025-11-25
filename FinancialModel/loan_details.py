import pandas as pd

loan_detail_df = pd.read_csv('FinancialModel/data/loan_details.csv')

def clean_loan_details(df):
    # ---- Normalize loan_type ----
    mapping = {
        'personal': 'personal',
        'personal loan': 'personal',
        'personalloan': 'personal',

        'creditcard': 'credit_card',
        'credit card': 'credit_card',
        'cc': 'credit_card',

        'mortgage': 'mortgage',

        'home loan': 'home',
        'homeloan': 'home',
        'home': 'home'
    }

    df['loan_type'] = df['loan_type'].apply(
        lambda x: mapping.get(str(x).strip().lower().replace("_", "").replace("-", " ").replace("  ", " "), 'other')
    )

    # ---- Clean loan_amount in-place ----
    df['loan_amount'] = (
        df['loan_amount']
        .astype(str)
        .str.replace(r'[\$,]', '', regex=True)
        .str.strip()
        .pipe(pd.to_numeric, errors='coerce')
        .astype("Int64")
    )

    # ---- Rename column ----
    df.rename(columns={'loan_amount': 'loan_amount ($)'}, inplace=True)

    return df

loan_detail_df = clean_loan_details(loan_detail_df)
loan_detail_df.to_csv('FinancialModel/data/sending/loan_details_cleaned.csv', index=False)
