import pandas as pd
from datetime import datetime
import re
import matplotlib.pyplot as plt
import seaborn as sns

# Define file paths
customers_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Customers.csv'
products_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Products.csv'
sales_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Sales.csv'
stores_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Stores.csv'
exchange_rates_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Exchange_Rates.csv'

# Load CSV files with appropriate encoding
customers_df = pd.read_csv(customers_file_path, encoding='ISO-8859-1')
products_df = pd.read_csv(products_file_path, encoding='ISO-8859-1')
sales_df = pd.read_csv(sales_file_path, encoding='ISO-8859-1')
stores_df = pd.read_csv(stores_file_path, encoding='ISO-8859-1')
exchange_rates_df = pd.read_csv(exchange_rates_file_path, encoding='ISO-8859-1')

# Print column names to debug
print("Customers DataFrame columns:", customers_df.columns)
print("Products DataFrame columns:", products_df.columns)
print("Sales DataFrame columns:", sales_df.columns)
print("Stores DataFrame columns:", stores_df.columns)
print("Exchange Rates DataFrame columns:", exchange_rates_df.columns)

# Data Cleaning and Preparation

# Handle missing values in 'State Code' column for customers_df
customers_df['State Code'].fillna('Unknown', inplace=True)
customers_df['Gender'].fillna('Unknown', inplace=True)
customers_df['City'].fillna('Unknown', inplace=True)

# Convert the 'Birthday' column to datetime format for customers_df
customers_df['Birthday'] = pd.to_datetime(customers_df['Birthday'], format='%m/%d/%Y', errors='coerce')

# Extract age from the 'Birthday' column by calculating the difference in days and dividing by 365.25 for customers_df
customers_df['Age'] = customers_df['Birthday'].apply(lambda x: (pd.to_datetime('2024-02-08') - x).days // 365.25 if pd.notnull(x) else None)

# Categorize age
def categorize_age(age):
    if pd.isnull(age):
        return 'Unknown'
    elif 20 <= age <= 40:
        return '20 to 40'
    elif 41 <= age <= 60:
        return '41 to 60'
    elif 61 <= age <= 80:
        return '61 to 80'
    elif age > 80:
        return '80 and above'
    else:
        return 'Unknown'

# Apply the categorize_age function to create a new column 'Age Category'
customers_df['Age Category'] = customers_df['Age'].apply(categorize_age)

# Handle missing values for products_df
products_df.fillna('Unknown', inplace=True)

# Convert numerical columns to float for products_df
if 'Unit Cost' in products_df.columns:
    products_df['Unit Cost'] = pd.to_numeric(products_df['Unit Cost'], errors='coerce')

if 'Unit Price' in products_df.columns:
    products_df['Unit Price'] = pd.to_numeric(products_df['Unit Price'], errors='coerce')

# Handle missing values in 'Delivery Date' column for sales_df
sales_df['Delivery Date'].fillna('Unknown', inplace=True)

# Convert date columns to datetime format for sales_df
sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'], format='%m/%d/%Y', errors='coerce')
sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'], format='%m/%d/%Y', errors='coerce')

# Function to clean state names
def clean_state_name(state_name):
    # Remove unwanted characters and fix common mistakes
    state_name = re.sub(r'[^\w\s]', '', state_name)  # Remove non-alphanumeric characters
    state_name = state_name.replace('La RÃ©union', 'La Réunion')  # Correct specific known mistakes
    state_name = state_name.replace('Ã©', 'é')  # Correct encoding issues
    state_name = re.sub(r'\d+', '', state_name)  # Remove numbers
    return state_name.strip()

# Clean state names in stores_df
stores_df['State'] = stores_df['State'].apply(clean_state_name)

# Convert date columns to datetime format for stores_df
stores_df['Open Date'] = pd.to_datetime(stores_df['Open Date'], format='%m/%d/%Y', errors='coerce')

# Fill missing values for stores_df
stores_df['Square Meters'].fillna('Unknown', inplace=True)

# Convert date columns to datetime format for exchange_rates_df
exchange_rates_df['Date'] = pd.to_datetime(exchange_rates_df['Date'], format='%m/%d/%Y', errors='coerce')

# Save cleaned DataFrames to CSV
customers_output_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Customers_file.csv'
customers_df.to_csv(customers_output_path, index=False)

products_output_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Products_file.csv'
products_df.to_csv(products_output_path, index=False)

sales_output_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Sales_file.csv'
sales_df.to_csv(sales_output_path, index=False)

stores_output_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Stores_file.csv'
stores_df.to_csv(stores_output_path, index=False)

exchange_rates_output_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Exchange_Rates_file.csv'
exchange_rates_df.to_csv(exchange_rates_output_path, index=False)

# Initialize merged_df with sales_df
merged_df = sales_df

# Merge Datasets step-by-step, checking for the presence of columns

# Example: Merging sales data with product data
if 'ProductKey' in sales_df.columns and 'ProductKey' in products_df.columns:
    merged_df = merged_df.merge(products_df, how='left', left_on='ProductKey', right_on='ProductKey')

# Merging with customer data with sales data
if 'CustomerKey' in sales_df.columns and 'CustomerKey' in customers_df.columns:
    merged_df = merged_df.merge(customers_df, how='left', left_on='CustomerKey', right_on='CustomerKey')

# Merging with store data with sales data
if 'StoreKey' in sales_df.columns and 'StoreKey' in stores_df.columns:
    merged_df = merged_df.merge(stores_df, how='left', left_on='StoreKey', right_on='StoreKey')

# Merging with exchange rates data
if 'Order Date' in sales_df.columns and 'Date' in exchange_rates_df.columns:
    merged_df = merged_df.merge(exchange_rates_df, how='left', left_on='Order Date', right_on='Date')

# Display the merged DataFrame information and the first few rows to verify the changes
print(merged_df.info())
print(merged_df.head())

# Save the merged DataFrame to a new CSV file
output_file_path = 'C:/Users/Sai.Vigneshwar/OneDrive - Collaborate 365/Sai data (30-05-23)/Desktop/Sai/Python/Guvi/DataSpark Illuminating Insights for Global Electronics/Save files/Merged_Data.csv'
merged_df.to_csv(output_file_path, index=False)


# EDA - Exploratory Data Analysis

# Univariate Analysis
# Histograms for numerical columns
merged_df.hist(bins=15, figsize=(15, 10), color='blue')
plt.suptitle('Histograms of Numerical Columns')
plt.show()

# Count plots for categorical columns
categorical_columns = merged_df.select_dtypes(include=['object']).columns
for col in categorical_columns:
    plt.figure(figsize=(10, 5))
    sns.countplot(y=merged_df[col], order=merged_df[col].value_counts().index)
    plt.title(f'Count plot for {col}')
    plt.show()


# 1. Country & Product
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='Country_x', hue='Category')
plt.title('Country_x & Category')
plt.xticks(rotation=45)
plt.show()

# 2. Country & Brand
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='Country_x', hue='Brand')
plt.title('Country_x & Brand')
plt.xticks(rotation=45)
plt.show()

# 3. Brand & Category
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='Category', hue='Brand')
plt.title('Category & Brand')
plt.xticks(rotation=45)
plt.show()

# 4. Category & Quantity
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='Category', hue='Quantity')
plt.title('Category & Quantity')
plt.xticks(rotation=45)
plt.show()


# 5. Category, Quantity & age category
plt.figure(figsize=(10, 6))
sns.barplot(data=merged_df, x='Category', y='Quantity', hue='Age Category', estimator=sum)
plt.title('Category & Quantity by Age Category')
plt.xticks(rotation=45)
plt.legend(title='Age Category')
plt.show()

# This option will show separate plots for each Age Category
g = sns.catplot(data=merged_df, x='Category', hue='Quantity', col='Age Category', kind='count', height=6, aspect=1.5)
g.set_xticklabels(rotation=45)
g.fig.suptitle('Category & Quantity by Age Category', y=1.02)
plt.show()

# 6. Country & Quantity
plt.figure(figsize=(10, 6))
sns.countplot(data=merged_df, x='Country_x', hue='Quantity')
plt.title('Country_x & Quantity')
plt.xticks(rotation=45)
plt.show()

# 7. Open Date & Country
plt.figure(figsize=(10, 30))
sns.countplot(data=merged_df, x='Country_x', hue='StoreKey')
plt.title('Country_x & StoreKey')
plt.xticks(rotation=45)
plt.show()


# 8. Agecategory & Country
plt.figure(figsize=(10, 30))
sns.countplot(data=merged_df, x='Country_x', hue='Age Category')
plt.title('Country_x & Age Category')
plt.xticks(rotation=45)
plt.show()


# 9. Heatmap of Country and Category
plt.figure(figsize=(12, 8))
country_category_pivot = pd.crosstab(merged_df['Country_x'], merged_df['Category'])
sns.heatmap(country_category_pivot, annot=True, fmt='d', cmap='Blues')
plt.title('Heatmap of Country and Category')
plt.show()

plt.figure(figsize=(12, 8))
country_category_pivot = pd.crosstab(merged_df['Brand'], merged_df['Category'])
sns.heatmap(country_category_pivot, annot=True, fmt='d', cmap='Blues')
plt.title('Heatmap of Brand and Category')
plt.show()

# Brand & gender
plt.figure(figsize=(12, 8))
country_category_pivot = pd.crosstab(merged_df['Brand'], merged_df['Gender'])
sns.heatmap(country_category_pivot, annot=True, fmt='d', cmap='Blues')
plt.title('Heatmap of Brand and Gender')
plt.show()

# Country & gender
plt.figure(figsize=(12, 8))
country_category_pivot = pd.crosstab(merged_df['Country_x'], merged_df['Gender'])
sns.heatmap(country_category_pivot, annot=True, fmt='d', cmap='Blues')
plt.title('Heatmap of Country_x and Gender')
plt.show()


# 10. #scatter plot Brand VS currency code
plt.figure(figsize=(12, 8))
sns.scatterplot(data=merged_df, x='Currency Code', y='Brand', hue='Currency Code')
plt.title('Scatter Plot of Brand vs. Currency Code by Currency Code')
plt.show()


# 11. Line plots
plt.figure(figsize=(12, 8))
sns.lineplot(data=merged_df, x='Brand', y='Gender', hue='Gender')
plt.title('Line Plot of Gender Over Time by Brand')
plt.xticks(rotation=45)
plt.show()
