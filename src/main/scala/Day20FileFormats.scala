import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day20FileFormats {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 20 - File Formats and Output")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    println("==============================================")
    println("DAY 20 - FILE FORMATS AND OUTPUT")
    println("==============================================")

    // ============================================================
    // 1. Daily Sales Data
    // ============================================================

    val salesData = Seq(
      ("S001", "2026-09-20", "P001", "Laptop", "Electronics", 75000.0),
      ("S002", "2026-09-20", "P002", "Mobile", "Electronics", 35000.0),
      ("S003", "2026-09-20", "P003", "Headphones", "Accessories", 5000.0),
      ("S004", "2026-09-21", "P004", "Tablet", "Electronics", 28000.0),
      ("S005", "2026-09-21", "P005", "Keyboard", "Accessories", 3000.0),
      ("S006", "2026-09-21", "P006", "Monitor", "Electronics", 22000.0),
      ("S007", "2026-09-22", "P007", "Mouse", "Accessories", 1500.0),
      ("S008", "2026-09-22", "P008", "Smart Watch", "Wearables", 12000.0),
      ("S009", "2026-09-22", "P009", "Camera", "Electronics", 45000.0),
      ("S010", "2026-09-23", "P010", "Printer", "Electronics", 18000.0)
    )

    val sales = salesData.toDF(
      "sale_id",
      "sale_date",
      "product_id",
      "product_name",
      "category",
      "amount"
    )

    val salesWithDate = sales
      .withColumn("date", to_date(col("sale_date")))
      .withColumn("year", year(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("day", dayofmonth(col("date")))

    println("\n===== SALES DATA =====")
    salesWithDate.show(false)

    // ============================================================
    // 2. CSV Write
    // ============================================================

    println("\n===== WRITING CSV =====")

    salesWithDate
      .drop("date")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv_sales")

    println("CSV output written to: output/csv_sales")

    // ============================================================
    // 3. JSON Write
    // ============================================================

    println("\n===== WRITING JSON =====")

    salesWithDate
      .drop("date")
      .coalesce(1)
      .write
      .mode("overwrite")
      .json("output/json_sales")

    println("JSON output written to: output/json_sales")

    // ============================================================
    // 4. Parquet Write
    // ============================================================

    println("\n===== WRITING PARQUET =====")

    salesWithDate
      .drop("date")
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet("output/parquet_sales")

    println("Parquet output written to: output/parquet_sales")

    // ============================================================
    // 5. Read CSV
    // ============================================================

    println("\n===== READING CSV =====")

    val csvData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("output/csv_sales")

    csvData.show(false)

    // ============================================================
    // 6. Read JSON
    // ============================================================

    println("\n===== READING JSON =====")

    val jsonData = spark.read
      .json("output/json_sales")

    jsonData.show(false)

    // ============================================================
    // 7. Read Parquet
    // ============================================================

    println("\n===== READING PARQUET =====")

    val parquetData = spark.read
      .parquet("output/parquet_sales")

    parquetData.show(false)

    // ============================================================
    // 8. Repartition Before Writing
    // ============================================================

    println("\n===== REPARTITION BEFORE WRITING =====")

    val repartitionedSales = salesWithDate.repartition(3)

    println(
      s"Partitions after repartition(3): ${repartitionedSales.rdd.getNumPartitions}"
    )

    repartitionedSales
      .drop("date")
      .write
      .mode("overwrite")
      .parquet("output/repartitioned_sales")

    println("Repartitioned output written to: output/repartitioned_sales")

    // ============================================================
    // 9. Partitioned Output: Year / Month / Day
    // ============================================================

    println("\n===== PARTITIONED OUTPUT =====")

    salesWithDate
      .drop("date")
      .repartition(3)
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/daily_sales")

    println("Partitioned output written to: output/daily_sales")

    // ============================================================
    // 10. Read Partitioned Data
    // ============================================================

    println("\n===== READING PARTITIONED OUTPUT =====")

    val dailySales = spark.read
      .parquet("output/daily_sales")

    dailySales.show(false)

    // ============================================================
    // 11. Partition Count
    // ============================================================

    println("\n===== PARTITION INFORMATION =====")

    println(
      s"Original partitions: ${salesWithDate.rdd.getNumPartitions}"
    )

    println(
      s"Repartitioned partitions: ${repartitionedSales.rdd.getNumPartitions}"
    )

    // ============================================================
    // 12. Daily Sales Summary
    // ============================================================

    println("\n===== DAILY SALES SUMMARY =====")

    salesWithDate
      .groupBy("year", "month", "day")
      .agg(
        count("sale_id").alias("sale_count"),
        sum("amount").alias("total_sales"),
        avg("amount").alias("average_sales")
      )
      .orderBy("year", "month", "day")
      .show()

    // ============================================================
    // 13. File Layout Explanation
    // ============================================================

    println("\n===== FILE LAYOUT EXPLANATION =====")

    println("Spark writes DataFrame output as a directory.")
    println("Each Spark partition can produce a part-* output file.")
    println("_SUCCESS indicates that the write completed successfully.")
    println("More partitions can result in more part-* files.")
    println("repartition() can increase or decrease the number of partitions.")
    println("coalesce() can reduce partitions with less shuffle.")
    println("partitionBy() creates directory partitions based on column values.")

    // ============================================================
    // 14. Partitioned Directory Example
    // ============================================================

    println("\n===== PARTITIONED DIRECTORY EXAMPLE =====")

    println("output/daily_sales/")
    println("├── year=2026/")
    println("│   └── month=9/")
    println("│       ├── day=20/")
    println("│       ├── day=21/")
    println("│       ├── day=22/")
    println("│       └── day=23/")
    println("└── _SUCCESS")

    // ============================================================
    // 15. File Format Comparison
    // ============================================================

    println("\n===== FILE FORMAT COMPARISON =====")

    println("CSV:")
    println("- Human-readable text format.")
    println("- Easy to exchange with other applications.")
    println("- Usually larger and slower than Parquet.")

    println("\nJSON:")
    println("- Semi-structured and human-readable.")
    println("- Useful for APIs and nested data.")
    println("- Usually larger than columnar formats.")

    println("\nParquet:")
    println("- Columnar storage format.")
    println("- Efficient for analytics.")
    println("- Supports compression and schema information.")
    println("- Commonly preferred for Spark data pipelines.")

    // ============================================================
    // 16. Scenario Explanation
    // ============================================================

    println("\n===== REAL-WORLD SCENARIO =====")

    println("Daily sales can be stored using year/month/day partitions.")
    println("For example, an application can read only one day's data")
    println("without scanning unrelated partition directories.")
    println("This can improve query efficiency for time-based analytics.")

    println("\n==============================================")
    println("DAY 20 FILE FORMATS AND OUTPUT COMPLETED")
    println("==============================================")

    spark.stop()
  }
}
