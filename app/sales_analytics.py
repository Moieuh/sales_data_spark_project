from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, year, month, date_format, sum as spark_sum,
    countDistinct, count, desc, round as spark_round, when, avg, datediff, max as spark_max,
    min as spark_min, regexp_replace
)


def show_df(title, df, n=20):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)
    df.show(n, truncate=False)


def main():
    spark = (
        SparkSession.builder
        .appName("Sales Data Analytics - Online Retail")
        .getOrCreate()
    )

    # Change this to /data/OnlineRetail.csv if you want to run locally in the Spark container.
    input_path = "hdfs://namenode:9000/data/OnlineRetail.csv"

    raw = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(input_path)
)

    show_df("Aperçu des données brutes", raw, 5)
    print("Nombre de lignes brutes:", raw.count())

    # Nettoyage : suppression des valeurs inutilisables, annulations et quantités/prix négatifs.
    cleaned = (
    raw
    .withColumn("Quantity", col("Quantity").cast("int"))
    .withColumn("UnitPrice", regexp_replace(col("UnitPrice").cast("string"), ",", ".").cast("double"))
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"))
    .withColumn("InvoiceDay", to_date(col("InvoiceDate")))
    .withColumn("TotalAmount", spark_round(col("Quantity") * col("UnitPrice"), 2))
    .filter(col("InvoiceNo").isNotNull())
    .filter(col("StockCode").isNotNull())
    .filter(col("Description").isNotNull())
    .filter(col("Quantity") > 0)
    .filter(col("UnitPrice") > 0)
    .filter(~col("InvoiceNo").cast("string").startswith("C"))
)

    show_df("Aperçu après nettoyage", cleaned, 5)
    print("Nombre de lignes nettoyées:", cleaned.count())

    # Attributs dérivés
    sales = (
        cleaned
        .withColumn("Year", year(col("InvoiceDate")))
        .withColumn("Month", month(col("InvoiceDate")))
        .withColumn("YearMonth", date_format(col("InvoiceDate"), "yyyy-MM"))
        .withColumn("Hour", date_format(col("InvoiceDate"), "HH"))
    )

    # KPI globaux
    kpis = sales.agg(
        spark_round(spark_sum("TotalAmount"), 2).alias("Total revenue"),
        countDistinct("InvoiceNo").alias("Number of orders"),
        countDistinct("CustomerID").alias("Number of customers"),
        countDistinct("StockCode").alias("Number of products"),
        spark_round(avg("TotalAmount"), 2).alias("Average line amount")
    ).withColumn(
        "Average basket value",
        spark_round(col("Total revenue") / col("Number of orders"), 2)
    )
    show_df("KPI globaux", kpis)

    # CA par mois
    revenue_by_month = (
        sales.groupBy("YearMonth")
        .agg(spark_round(spark_sum("TotalAmount"), 2).alias("Revenue"), countDistinct("InvoiceNo").alias("Orders"))
        .orderBy("YearMonth")
    )
    show_df("Chiffre d'affaires par mois", revenue_by_month, 50)

    # Top produits par chiffre d'affaires
    top_products_revenue = (
        sales.groupBy("StockCode", "Description")
        .agg(
            spark_round(spark_sum("TotalAmount"), 2).alias("Revenue"),
            spark_sum("Quantity").alias("Quantity sold"),
            countDistinct("InvoiceNo").alias("Orders")
        )
        .orderBy(desc("Revenue"))
    )
    show_df("Top 10 produits par chiffre d'affaires", top_products_revenue, 10)

    # Top produits par volume vendu
    top_products_quantity = (
        sales.groupBy("StockCode", "Description")
        .agg(spark_sum("Quantity").alias("Quantity sold"), spark_round(spark_sum("TotalAmount"), 2).alias("Revenue"))
        .orderBy(desc("Quantity sold"))
    )
    show_df("Top 10 produits par quantité vendue", top_products_quantity, 10)

    # Analyse par pays
    country_sales = (
        sales.groupBy("Country")
        .agg(
            spark_round(spark_sum("TotalAmount"), 2).alias("Revenue"),
            countDistinct("InvoiceNo").alias("Orders"),
            countDistinct("CustomerID").alias("Customers")
        )
        .withColumn("Average basket", spark_round(col("Revenue") / col("Orders"), 2))
        .orderBy(desc("Revenue"))
    )
    show_df("Ventes par pays", country_sales, 20)

    # Meilleurs clients
    top_customers = (
        sales.filter(col("CustomerID").isNotNull())
        .groupBy("CustomerID", "Country")
        .agg(
            spark_round(spark_sum("TotalAmount"), 2).alias("Revenue"),
            countDistinct("InvoiceNo").alias("Orders"),
            spark_round(avg("TotalAmount"), 2).alias("Average line amount")
        )
        .orderBy(desc("Revenue"))
    )
    show_df("Top 10 clients par chiffre d'affaires", top_customers, 10)

    # RFM simple : Recency, Frequency, Monetary
    from pyspark.sql.functions import lit
    max_date = sales.agg(spark_max("InvoiceDay").alias("max_date")).collect()[0]["max_date"]
    rfm = (
        sales.filter(col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(
            countDistinct("InvoiceNo").alias("Frequency"),
            spark_round(spark_sum("TotalAmount"), 2).alias("Monetary"),
            spark_max("InvoiceDay").alias("LastPurchaseDate")
        )
        .withColumn("RecencyDays", datediff(lit(max_date), col("LastPurchaseDate")))
        .withColumn(
            "Segment",
            when((col("Frequency") >= 10) & (col("Monetary") >= 1000), "VIP")
            .when(col("Frequency") >= 5, "Fidèle")
            .when(col("RecencyDays") <= 30, "Récent")
            .otherwise("À réactiver")
        )
    )
    show_df("Segmentation client RFM simple", rfm.orderBy(desc("Monetary")), 10)

    # Sauvegarde des résultats en CSV
    revenue_by_month.coalesce(1).write.mode("overwrite").option("header", True).csv("/output/revenue_by_month")
    top_products_revenue.coalesce(1).write.mode("overwrite").option("header", True).csv("/output/top_products_revenue")
    country_sales.coalesce(1).write.mode("overwrite").option("header", True).csv("/output/country_sales")
    top_customers.coalesce(1).write.mode("overwrite").option("header", True).csv("/output/top_customers")
    rfm.coalesce(1).write.mode("overwrite").option("header", True).csv("/output/rfm_customers")

    print("\nRésultats sauvegardés dans le dossier output/.")
    spark.stop()


if __name__ == "__main__":
    main()
