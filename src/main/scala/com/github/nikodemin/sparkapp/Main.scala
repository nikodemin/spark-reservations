package com.github.nikodemin.sparkapp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark app")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val merchantSchema = new StructType()
      .add("merchant_id", StringType)
      .add("merchant_name", StringType)

    val reservationSchema = new StructType()
      .add("reservation_id", StringType)
      .add("merchant_id", StringType)
      .add("guest_count", LongType)
      .add("merchant_email", StringType)
      .add("starts_at", DateType)
      .add("ends_at", DateType)
      .add("created_at", DateType)
      .add("has_promo_code", BooleanType)
      .add("customer_id", StringType)

    val merchantDf = spark.read
      .option("delimiter", ";")
      .schema(merchantSchema)
      .csv("src/main/resources/merchant_dataset.csv")

    val reservationDf = spark.read
      .option("delimiter", ";")
      .schema(reservationSchema)
      .csv("src/main/resources/reservation_dataset.csv")

    merchantDf.printSchema
    merchantDf.show
    reservationDf.printSchema
    reservationDf.show
  }
}
