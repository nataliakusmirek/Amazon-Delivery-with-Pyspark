# Amazon Delivery Data Analysis

## Overview

This project involves analyzing and visualizing Amazon delivery data using PySpark. The goal is to identify patterns in delivery performance and gain insights into delivery trends. The analysis includes exploring delivery times, top purchase categories, and delivery performance based on weather conditions and vehicle types.

## Objectives

- Identify patterns in delivery performance.
- Visualize trends and insights from the data.

## Dataset

The dataset used in this project is the [Amazon Delivery Dataset](https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset/data) from Kaggle. It contains information about delivery orders including timestamps, vehicle types, weather conditions, and delivery times.

## Project Structure

```
amazon_delivery_analysis/
│
├── data/
│   └── amazon_delivery_data.csv
├── scripts/
│   └── main.py
└── README.md
```

- `data/`: Directory containing the dataset.
- `scripts/`: Python script with PySpark code for data analysis.
- `README.md`: This file.

## Requirements

- Python 3.x
- PySpark
- pandas
- matplotlib
- seaborn

Install the required libraries using:

```bash
pip install pyspark pandas matplotlib seaborn
```

## Usage

1. **Load the Data**: Load the dataset into a PySpark DataFrame.

2. **Data Exploration**: Explore the data, check for missing values, and clean the dataset.

3. **Analysis**:
   - **Top Purchase Categories**: Identify the most popular purchase categories in different areas.
   - **Average Delivery Time by Weather**: Calculate the average delivery time for each weather condition.
   - **Average Delivery Time by Vehicle Type**: Calculate the average delivery time based on vehicle type.

4. **Visualization**:
   - Visualize the number of deliveries by vehicle type, weather condition, and category.
   - Plot the distribution of average delivery times.

5. **Run the Script**: Execute the Python script to perform the analysis and generate visualizations.

   ```bash
   python scripts/data_analysis.py
   ```

## Code Example

Here is a snippet of the PySpark code used for analysis:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, unix_timestamp
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder.appName("Amazon Delivery Data Analysis").getOrCreate()

# Load the dataset
df = spark.read.csv("data/amazon_delivery_data.csv", header=True, inferSchema=True)

# ...

# Example analysis
result = df.groupBy("Area", "Category").count().orderBy(col("count").desc())
result.show()
```

## Results

The analysis provides insights into delivery performance, including:

- Top purchase categories by area.
- Average delivery times based on weather conditions and vehicle types.
- Visual trends and distributions related to deliveries.

## Acknowledgments

- [Kaggle](https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset/data) for providing the dataset.
- PySpark, pandas, matplotlib, and seaborn for their powerful data analysis and visualization capabilities.
