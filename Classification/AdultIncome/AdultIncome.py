import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Load dataset
adult_df = pd.read_csv('Classification/data/AdultIncome/adult.csv')

# ----------------------------
# 1. Cleaning function
# ----------------------------
def cleaning_data(df: pd.DataFrame):
    df = df.drop_duplicates().copy()
    
    # Encode binary categorical features manually
    df['sex'] = df['sex'].replace({'Male': 1, 'Female': 0}).astype(int)
    df['income'] = df['income'].replace({'<=50K': 1, '>50K': 0}).astype(int)
    
    # Fill missing values if any
    df = df.replace('?', np.nan)
    df = df.fillna(df.mode().iloc[0])  # mode imputation for categorical columns
    
    return df

cleaned_df = cleaning_data(adult_df)

# ----------------------------
# 2. Split target and input
# ----------------------------
target_df = cleaned_df['income']
input_df = cleaned_df.drop('income', axis=1)

# Identify column types
numeric_cols = input_df.select_dtypes(include=np.number).columns.tolist()
categoric_cols = input_df.select_dtypes(include='object').columns.tolist()

# ----------------------------
# 3. Normalize and encode
# ----------------------------
def normalizedDataFrame(df: pd.DataFrame):
    df = df.copy()
    
    scaler = MinMaxScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    encoded = encoder.fit_transform(df[categoric_cols])
    encoded_cols = encoder.get_feature_names_out(categoric_cols)
    
    encoded_df = pd.DataFrame(encoded, columns=encoded_cols, index=df.index)
    
    final_df = pd.concat([df[numeric_cols], encoded_df], axis=1)
    return final_df

normalized_df = normalizedDataFrame(input_df)

# ----------------------------
# 4. Split dataset
# ----------------------------
X_train, X_test, y_train, y_test = train_test_split(
    normalized_df, target_df, random_state=42, train_size=0.7
)

# ----------------------------
# 5. Train and compare models
# ----------------------------
models = {
    "Logistic Regression": LogisticRegression(max_iter=500),
    "Decision Tree": DecisionTreeClassifier(random_state=42),
    "Random Forest": RandomForestClassifier(random_state=42, n_estimators=100)
}

for name, model in models.items():
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    print(f"{name} Accuracy: {acc:.4f}")
