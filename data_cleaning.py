import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Sample DataFrame with common issues
data = {
    'Name': ['Alice', 'Bob', 'alice', 'Charlie', 'bob', None],
    'Age': [25, np.nan, 25, 35, 25, 22],
    'City': ['New York', 'new york', 'New Yrok', 'Los Angeles', 'LOS ANGELES', 'Chicago'],
    'Salary': ['$1000', '$1500', '$1000', '$2000', '$1500', '$1800'],
    'JoinDate': ['2020-01-01', '2021/02/15', '01-03-2022', 'April 4, 2023', '2024.05.05', None],
    'Score': [95, 90, 92, 200, 91, 88],  # Outlier: 200
}

df = pd.DataFrame(data)
print("Original DataFrame:\n", df)

# -------------------------------------------
# 1. Handle Missing Values
# -------------------------------------------
df['Age'].fillna(df['Age'].mean(), inplace=True)
df['Name'].fillna('Unknown', inplace=True)

# -------------------------------------------
# 2. Remove Duplicates (if any)
# -------------------------------------------
df.drop_duplicates(inplace=True)

# -------------------------------------------
# 3. Fix Inconsistent Data
# Standardize name and city
df['Name'] = df['Name'].str.title().str.strip()
df['City'] = df['City'].str.title().str.strip()
df['City'].replace({'New Yrok': 'New York'}, inplace=True)

# -------------------------------------------
# 4. Handle Outliers (IQR method for 'Score')
Q1 = df['Score'].quantile(0.25)
Q3 = df['Score'].quantile(0.75)
IQR = Q3 - Q1
df = df[(df['Score'] >= Q1 - 1.5 * IQR) & (df['Score'] <= Q3 + 1.5 * IQR)]

# -------------------------------------------
# 5. Standardize Formats
# Convert Salary to numeric
df['Salary'] = df['Salary'].replace('[\$,]', '', regex=True).astype(float)

# Convert JoinDate to datetime
df['JoinDate'] = pd.to_datetime(df['JoinDate'], errors='coerce')

# -------------------------------------------
# 6. Fix Data Entry Errors (City)
print("\nCity value counts before correction:")
print(df['City'].value_counts())

# Already fixed above with replace, but here’s how you’d detect
# Check for unique values:
print("\nUnique Cities:\n", df['City'].unique())

# -------------------------------------------
# 7. Correct Data Types
print("\nData Types Before:\n", df.dtypes)
df['Age'] = df['Age'].astype(int)
print("\nData Types After:\n", df.dtypes)

# -------------------------------------------
# 8. Drop Irrelevant Features
# Let's say we don’t need the 'JoinDate' column
df.drop(['JoinDate'], axis=1, inplace=True)

# -------------------------------------------
# Final Cleaned DataFrame
print("\nCleaned DataFrame:\n", df)
