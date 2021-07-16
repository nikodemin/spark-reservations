package com.github.nikodemin.sparkapp

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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

    val emailVerification = udf((email: String) => email.matches("(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])"))

    val validatedReservationDf = reservationDf.filter(emailVerification(col("merchant_email"))).cache

    println(s"Reservations count: ${reservationDf.count}, valid: ${validatedReservationDf.count}," +
      s" invalid: ${reservationDf.count - validatedReservationDf.count}")

    validatedReservationDf.agg(avg("guest_count")).show

    println("Merchant with top guests count:")
    validatedReservationDf.filter(col("guest_count").gt(1))
      .groupBy("merchant_id")
      .agg(sum("guest_count").as("guests_sum"))
      .orderBy(col("guests_sum").desc)
      .limit(1)
      .join(merchantDf, "merchant_id")
      .show

    val quarters = Vector("January, February, March", "April, May, June", "July, August, September",
      "October, November, December")
    val getQuarter = udf((date: Date) => quarters((date.toLocalDate.getMonthValue - 1) / 3))

    val reservationsByQuartersDf = validatedReservationDf
      .groupBy(getQuarter(col("created_at")).as("quarter"), col("merchant_id"))
      .agg(count("*").as("reservations"))
      .cache

    println("Merchants with top guests count by quarters:")
    reservationsByQuartersDf.as("a").join(reservationsByQuartersDf.as("b"),
      expr("a.quarter = b.quarter AND a.reservations < b.reservations")
      , "left")
      .where("b.reservations is NULL")
      .select("a.*")
      .join(merchantDf, "merchant_id")
      .show

    validatedReservationDf.unpersist(true)
    reservationsByQuartersDf.unpersist(true)
  }
}
