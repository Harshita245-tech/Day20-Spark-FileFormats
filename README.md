# 🚀 Day 20 - Apache Spark File Formats and Output

## 📌 Overview

This project demonstrates how Apache Spark reads and writes different file formats and how Spark manages output partitions.

The project covers:

- CSV read and write
- JSON read and write
- Parquet read and write
- `repartition()`
- `coalesce()`
- Partitioned output using `year`, `month`, and `day`
- Spark output file layout
- Number of output files
- Daily sales data processing

The scenario represents a real-world data engineering pipeline where daily sales data is stored in partitioned files for efficient analytical processing.

---

## 🎯 Objectives

- Understand Spark file input and output operations
- Read and write CSV files
- Read and write JSON files
- Read and write Parquet files
- Understand Spark output directories
- Understand `part-*` files
- Understand Spark partitions
- Practice `repartition()` before writing
- Understand the difference between `repartition()` and `coalesce()`
- Write data partitioned by year, month, and day
- Analyze daily sales data

---

## 🛠️ Technologies Used

- **Apache Spark 3.5.3**
- **Scala 2.12.18**
- **Spark SQL**
- **SBT**
- **Ubuntu/Linux**
- **Git & GitHub**

---

## 📂 Project Structure

    day20-spark/
    │
    ├── src/
    │   └── main/
    │       └── scala/
    │           └── Day20FileFormats.scala
    │
    ├── project/
    │   └── build.properties
    │
    ├── build.sbt
    ├── .gitignore
    └── README.md

Generated Spark output is intentionally not committed to GitHub.

---

## 📊 Scenario - Daily Sales

The project uses a daily sales dataset containing:

- Sale ID
- Sale Date
- Product ID
- Product Name
- Category
- Sales Amount

Example:

| Sale ID | Sale Date | Product | Category | Amount |
|---|---|---|---|---:|
| S001 | 2026-09-20 | Laptop | Electronics | 75000 |
| S002 | 2026-09-20 | Mobile | Electronics | 35000 |
| S003 | 2026-09-20 | Headphones | Accessories | 5000 |
| S004 | 2026-09-21 | Tablet | Electronics | 28000 |
| S005 | 2026-09-21 | Keyboard | Accessories | 3000 |

---

# 📄 CSV

CSV stands for **Comma-Separated Values**.

It is a simple text-based format that is commonly used for data exchange.

### Writing CSV

    salesWithDate
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv_sales")

### Reading CSV

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("output/csv_sales")

### Advantages

- Easy to read
- Easy to exchange
- Supported by many tools
- Human-readable

### Limitations

- Larger file size
- No efficient columnar storage
- Schema information is not inherently stored
- Usually less efficient for analytical workloads

---

# 🧾 JSON

JSON stands for **JavaScript Object Notation**.

It is a semi-structured format commonly used by APIs and applications.

### Writing JSON

    salesWithDate
      .write
      .mode("overwrite")
      .json("output/json_sales")

### Reading JSON

    spark.read
      .json("output/json_sales")

### Advantages

- Human-readable
- Supports semi-structured data
- Useful for APIs
- Supports nested structures

### Limitations

- Larger than columnar formats
- Can require more storage
- Generally less efficient than Parquet for analytics

---

# 🗃️ Parquet

Parquet is a **columnar storage format** widely used in big-data systems.

### Writing Parquet

    salesWithDate
      .write
      .mode("overwrite")
      .parquet("output/parquet_sales")

### Reading Parquet

    spark.read
      .parquet("output/parquet_sales")

### Advantages

- Columnar storage
- Efficient analytical queries
- Supports compression
- Stores schema information
- Works efficiently with Spark
- Allows column pruning

For Spark-based analytical pipelines, Parquet is often preferred over CSV and JSON.

---

# 🔄 Repartition

`repartition()` changes the number of Spark partitions.

Example:

    val repartitionedSales =
      salesWithDate.repartition(3)

The project verifies the partition count using:

    repartitionedSales.rdd.getNumPartitions

### Important Point

`repartition()` can increase or decrease the number of partitions and generally involves a shuffle.

    Original Data
         ↓
    repartition(3)
         ↓
       Shuffle
         ↓
      3 Partitions

---

# 🔽 Coalesce

`coalesce()` is commonly used to reduce the number of partitions.

Example:

    salesWithDate.coalesce(1)

Unlike `repartition()`, `coalesce()` can reduce partitions with less shuffle.

It is useful when a smaller number of output files is required.

---

# 📁 Spark Output File Layout

Spark writes DataFrame output as a **directory**, not as a single ordinary file.

For example:

    output/parquet_sales/
    ├── part-00000-....snappy.parquet
    ├── _SUCCESS

If there are multiple Spark partitions, Spark can generate multiple `part-*` files.

Example:

    output/repartitioned_sales/
    ├── part-00000-....parquet
    ├── part-00001-....parquet
    ├── part-00002-....parquet
    └── _SUCCESS

---

# 🔢 Number of Output Files

The number of `part-*` files is related to the number of Spark partitions used when writing.

For example:

    salesWithDate
      .repartition(3)
      .write
      .parquet("output/repartitioned_sales")

can produce approximately:

    3 part-* files

The exact physical output can depend on Spark execution and whether partitions contain data.

---

# 🗂️ Partitioned Output

Spark supports partitioned storage using:

    .partitionBy("year", "month", "day")

The project writes daily sales data using:

    salesWithDate
      .drop("date")
      .repartition(3)
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/daily_sales")

This creates a directory structure based on the partition columns.

---

## 📂 Partitioned Directory Example

    output/daily_sales/
    │
    ├── year=2026/
    │   └── month=9/
    │       ├── day=20/
    │       ├── day=21/
    │       ├── day=22/
    │       └── day=23/
    │
    └── _SUCCESS

This layout allows Spark to identify data based on the partition columns.

---

# 🚀 Why Partition Data?

Partitioning is useful when queries frequently filter by partition columns.

For example:

    year = 2026
    month = 9
    day = 22

Spark can use the directory structure to avoid scanning unrelated partitions.

This technique is called **partition pruning**.

---

# 📅 Daily Sales Scenario

A real-world sales pipeline may receive data every day:

    Daily Sales
         ↓
    Spark Processing
         ↓
    Add Year / Month / Day
         ↓
    Partitioned Parquet
         ↓
    Data Lake

Example:

    sales/
    ├── year=2026/
    │   ├── month=9/
    │   │   ├── day=20/
    │   │   ├── day=21/
    │   │   ├── day=22/
    │   │   └── day=23/

This structure is useful for:

- Daily reporting
- Monthly reporting
- Data lake storage
- Business intelligence
- Historical analysis

---

# 📊 Daily Sales Summary

The project calculates daily sales metrics using:

    salesWithDate
      .groupBy("year", "month", "day")
      .agg(
        count("sale_id").alias("sale_count"),
        sum("amount").alias("total_sales"),
        avg("amount").alias("average_sales")
      )
      .orderBy("year", "month", "day")

The output contains:

- Sale count
- Total sales
- Average sales

for each day.

---

# ⚔️ CSV vs JSON vs Parquet

| Feature | CSV | JSON | Parquet |
|---|---|---|---|
| Format | Tabular text | Semi-structured | Columnar |
| Human-readable | Yes | Yes | No |
| Schema | Not inherent | Partially inferred | Stored |
| Compression | Limited | Limited | Excellent support |
| Analytics | Moderate | Moderate | Excellent |
| Spark performance | Lower | Moderate | High |
| Nested data | No | Yes | Yes |
| Common data-lake use | Limited | Moderate | High |

---

# ⚔️ Repartition vs Coalesce

| Feature | Repartition | Coalesce |
|---|---|---|
| Changes partition count | Yes | Yes |
| Can increase partitions | Yes | Generally no |
| Can decrease partitions | Yes | Yes |
| Shuffle | Usually yes | Usually less/no full shuffle |
| Use case | Redistribute data | Reduce partitions |
| Useful before writing | Yes | Yes |

---

# 🧠 Key Concepts Learned

### 1. File Formats

Spark can read and write multiple file formats including CSV, JSON, and Parquet.

### 2. Partitions

Spark divides distributed data into partitions for parallel processing.

### 3. Repartition

`repartition()` redistributes data into a specified number of partitions.

### 4. Coalesce

`coalesce()` is useful for reducing the number of partitions and output files.

### 5. Partitioned Storage

`partitionBy()` organizes data into directory structures based on column values.

### 6. Output Files

Each Spark partition can produce a `part-*` file during a write operation.

### 7. Parquet

Parquet is a columnar format well suited for analytical workloads.

---

# 🌍 Real-World Applications

These concepts are commonly used in:

- 🛒 E-commerce data lakes
- 🏦 Banking systems
- 📊 Business intelligence
- 🚚 Supply chain analytics
- 🏭 Manufacturing analytics
- ✈️ Travel and booking systems
- 💳 Financial transaction processing
- 📈 Daily sales reporting

---

# ▶️ How to Run

### 1. Clone the repository

    git clone https://github.com/Harshita245-tech/Day20-Spark-FileFormats.git

### 2. Move into the project

    cd Day20-Spark-FileFormats

### 3. Compile

    sbt compile

### 4. Run

    sbt run

---

# 📌 Main File

    src/main/scala/Day20FileFormats.scala

---

# 📚 Day 20 Learning Summary

The main data flow demonstrated in this project is:

    Daily Sales Data
           ↓
    Spark DataFrame
           ↓
    Add Year / Month / Day
           ↓
      ┌────┼─────────┐
      ↓    ↓         ↓
     CSV  JSON    Parquet
           ↓
      Repartition
           ↓
    Partitioned Output
           ↓
       year/month/day

The exercise demonstrates how Spark manages file formats, partitions, output files, and partitioned data storage in a practical data engineering scenario.

---

## ✅ Project Status

**Day 20 - File Formats and Output: COMPLETED 🎉**
