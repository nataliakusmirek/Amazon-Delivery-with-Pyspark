from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year
import pandas
import matplotlib.pyplot as plt
import seaborn as sns

# Objectives:
# Identify patterns in delivery performance.
# Visualize trends and insights from the data.

# Load the data
spark = SparkSession.builder.appName("Amazon Delivery Data Analysis").getOrCreate()
df = spark.read.csv("amazon_delivery_data.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

# Data Exploration
print("Number of rows: ", df.count())
print("Number of columns: ", len(df.columns))
print("Column names: ", df.columns)

# Check for missing values
df = df.dropna()



# Identify patterns in delivery performance

# Top purchase categories from the Metropolitan Area
df = df.withColumn("Area", when(col("Area") == "Metropolitian", "Metropolitian").otherwise(col("Area")))
result = df.groupBy("Area", "Category").count() \
    .orderBy("count", ascending=False)
result.show()

# Average delivery time based on each weather condition
df = df.withColumn("Average_Delivery_Time", (col("Pickup_Time") - col("Order_Time")) / 2)
result = df.groupBy("Weather").agg(avg("Average_Delivery_Time").alias("Avg_Delivery_Time"))
result.show()

# Average delivery time based on vehicle type
df = df.withColumn("Average_Delivery_Time", (col("Pickup_Time") - col("Order_Time")) / 2)
result = df.groupBy("Vehicle").agg(avg("Average_Delivery_Time").alias("Avg_Delivery_Time"))
result.show()

# Visualize trends and insights from the data
df_pandas = df.toPandas()

plt.figure(figsize=(10, 6))
sns.countplot(data=df_pandas, x='Vehicle')
plt.title("Number of Deliveries by Vehicle Type")
plt.show()

plt.figure(figsize=(10, 6))
sns.countplot(data=df_pandas, x='Weather')
plt.title("Number of Deliveries by Weather Condition")
plt.show()

plt.figure(figsize=(10, 6))
sns.countplot(data=df_pandas, x='Category')
plt.title("Number of Deliveries by Category")
plt.show()

plt.figure(figsize=(10, 6))
sns.countplot(data=df_pandas, x='Area')
plt.title("Number of Deliveries by Area")
plt.show()

plt.figure(figsize=(10, 6))
sns.histplot(data=df_pandas, x='Average_Delivery_Time', bins=30)
plt.title("Distribution of Average Delivery Time")
plt.show()

# Stop the Spark session
spark.stop()
