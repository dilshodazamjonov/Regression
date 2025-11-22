import pandas as pd

demographic_df = pd.read_csv('FinancialModel/data/demographics (1) (2).csv')

def clean_demographics(df):

    # ---- Rename columns ----
    df = df.rename(columns={
        'cust_id': 'customer_id',
        'annual_income': 'annual_income ($)'
    })

    # ---- Clean annual income ----
    df['annual_income ($)'] = (
        df['annual_income ($)']
        .astype(str)
        .str.replace(r'[\$,]', '', regex=True)
        .str.strip()
        .pipe(pd.to_numeric, errors='coerce')
        .astype("Int64")
    )

    # ---- 3. Employment type mapping ----
    mapping = {
        'full-time': 'full_time',
        'full time': 'full_time',
        'fulltime': 'full_time',
        'ft': 'full_time',
        'full_time': 'full_time',

        'part-time': 'part_time',
        'part time': 'part_time',
        'parttime': 'part_time',
        'pt': 'part_time',
        'part_time': 'part_time',

        'contractor': 'contract',
        'contract': 'contract',
        'contractor ': 'contract',

        'self employed': 'self_employed',
        'self-employed': 'self_employed',
        'self employed ': 'self_employed',
        'selfemp': 'self_employed',
        'self emp': 'self_employed',
        'self_employed': 'self_employed'
    }

    cleaned_employment = []
    for value in df['employment_type']:
        key = (
            str(value)
            .strip()
            .lower()
            .replace("_", " ")
            .replace("-", " ")
        )
        key = " ".join(key.split())
        cleaned_employment.append(mapping.get(key, 'other'))

    df['employment_type'] = cleaned_employment

    # ---- 4. Fill missing employment_length with median per employment_type ----
    df['employment_length'] = df['employment_length'].fillna(
        df.groupby('employment_type')['employment_length'].transform('median')
    )

    return df


demographic_df = clean_demographics(demographic_df)

demographic_df.to_csv('FinancialModel/data/sending/demographic_cleaned.csv', index=False)

demographic_df.head(10)
