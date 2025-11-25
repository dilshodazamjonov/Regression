import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
import logging
import warnings


warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)


logging.basicConfig(
    filename='LoanPrediction.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

loanPred_df: pd.DataFrame = pd.read_csv('Classification/data/train_u6lujuX_CVtuZ9i (1).csv')

input_data = loanPred_df[loanPred_df.columns[1:-1]]
target_col = loanPred_df[loanPred_df.columns[-1]]

numeric_cols = input_data.select_dtypes(include=np.number).columns
categorical_cols = input_data.select_dtypes(include='object').columns

def cleaning_data(df: pd.DataFrame):
    boolean = {'Yes': 1, 'No': 0}
    df['Married'].replace(boolean, inplace=True)
    df['Self_Employed'].replace(boolean, inplace=True)

    boolean_gender = {'Female': 1, 'Male': 0}
    df['Gender'].replace(boolean_gender, inplace=True)

    boolean_grad = {'Graduate': 1, 'Not Graduate': 0}
    df['Education'].replace(boolean_grad, inplace=True)

    df['Dependents'].replace({'3+': 3, '1': 1, '2': 2, '0': 0}, inplace=True)

    binary_cols = ['Gender', 'Married', 'Self_Employed', 'Dependents', 'Education']
    for col in binary_cols:
        df[col].fillna(0, inplace=True)
        df[col] = df[col].astype(int)
    return df

def filling_missing_data(df: pd.DataFrame):
    logging.info("Starting cleaning pipeline...")
    df = cleaning_data(df)
    for col in numeric_cols:
        df[col].fillna(df[col].mean(), inplace=True)
    for col in categorical_cols:
        df[col].fillna('Unknown', inplace=True)
    return df

def normalized_df(df: pd.DataFrame):
    norm_df = filling_missing_data(df)
    scaler = MinMaxScaler().fit(norm_df[numeric_cols])
    norm_df[numeric_cols] = scaler.transform(norm_df[numeric_cols])

    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    encoded = encoder.fit_transform(norm_df[['Property_Area']])
    encoded_cols = encoder.get_feature_names_out(['Property_Area'])
    encoded_df = pd.DataFrame(encoded, columns=encoded_cols, index=norm_df.index)
    norm_df = pd.concat([norm_df.drop('Property_Area', axis=1), encoded_df], axis=1)

    logging.info(f"Normalized DataFrame head:\n{norm_df.head()}")
    return norm_df


input_data = normalized_df(input_data)


X_train, X_test, y_train, y_test = train_test_split(input_data, target_col, test_size=0.3, random_state=42)

logisticReg = LogisticRegression()

logisticReg.fit(X_train, y_train)

predictions = logisticReg.predict(X_test)

try:
    res = pd.DataFrame({
        'actual_test': y_test,
        'predicted_value': predictions,
        'Model Training Score': logisticReg.score(X_test, y_test),
    }).to_csv('Classification/data/test_output.csv')
    print("Done")

except Exception as e:
    print(e)



print(f"Model Score for a Trained data {logisticReg.score(X_train, y_train)}")
print(f"Model score for a Testing Data {logisticReg.score(X_test, y_test)}")
print(confusion_matrix(y_test, predictions))