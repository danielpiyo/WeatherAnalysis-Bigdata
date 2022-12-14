package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * @author ${user.name}
 */
object App {

  //  creating spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("WeatherAnalysis01 Humidity")
    .master("local")
    .getOrCreate()

  //    creating schema
  val temperature_schema: StructType = StructType(Array(
    StructField("datetime", DateType),
    StructField("Vancouver", DoubleType),
    StructField("Portland", DoubleType),
    StructField("San Francisco", DoubleType),
    StructField("Seattle", DoubleType),
    StructField("Los Angeles", DoubleType),
    StructField("San Diego", DoubleType),
    StructField("Las Vegas", DoubleType),
    StructField("Phoenix", DoubleType),
    StructField("Albuquerque", DoubleType),
    StructField("Denver", DoubleType),
    StructField("San Antonio", DoubleType),
    StructField("Dallas", DoubleType),
    StructField("Houston", DoubleType),
    StructField("Kansas City", DoubleType),
    StructField("Minneapolis", DoubleType),
    StructField("Saint Louis", DoubleType),
    StructField("Chicago", DoubleType),
    StructField("Nashville", DoubleType),
    StructField("Indianapolis", DoubleType),
    StructField("Atlanta", DoubleType),
    StructField("Detroit", DoubleType),
    StructField("Jacksonville", DoubleType),
    StructField("Charlotte", DoubleType),
    StructField("Miami", DoubleType),
    StructField("Pittsburgh", DoubleType),
    StructField("Toronto", DoubleType),
    StructField("Philadelphia", DoubleType),
    StructField("New York", DoubleType),
    StructField("Montreal", DoubleType),
    StructField("Boston", DoubleType),
    StructField("Beersheba", DoubleType),
    StructField("Tel Aviv District", DoubleType),
    StructField("Eilat", DoubleType),
    StructField("Haifa", DoubleType),
    StructField("Nahariyya", DoubleType),
    StructField("Jerusalem", DoubleType)
  ))
  val humidity_schema: StructType = StructType(Array(
    StructField("datetime", DateType),
    StructField("Vancouver", IntegerType),
    StructField("Portland", IntegerType),
    StructField("San Francisco", IntegerType),
    StructField("Seattle", IntegerType),
    StructField("Los Angeles", IntegerType),
    StructField("San Diego", IntegerType),
    StructField("Las Vegas", IntegerType),
    StructField("Phoenix", IntegerType),
    StructField("Albuquerque", IntegerType),
    StructField("Denver", IntegerType),
    StructField("San Antonio", IntegerType),
    StructField("Dallas", IntegerType),
    StructField("Houston", IntegerType),
    StructField("Kansas City", IntegerType),
    StructField("Minneapolis", IntegerType),
    StructField("Saint Louis", IntegerType),
    StructField("Chicago", IntegerType),
    StructField("Nashville", IntegerType),
    StructField("Indianapolis", IntegerType),
    StructField("Atlanta", IntegerType),
    StructField("Detroit", IntegerType),
    StructField("Jacksonville", IntegerType),
    StructField("Charlotte", IntegerType),
    StructField("Miami", IntegerType),
    StructField("Pittsburgh", IntegerType),
    StructField("Toronto", IntegerType),
    StructField("Philadelphia", IntegerType),
    StructField("New York", IntegerType),
    StructField("Montreal", IntegerType),
    StructField("Boston", IntegerType),
    StructField("Beersheba", IntegerType),
    StructField("Tel Aviv District", IntegerType),
    StructField("Eilat", IntegerType),
    StructField("Haifa", IntegerType),
    StructField("Nahariyya", IntegerType),
    StructField("Jerusalem", IntegerType)
  ))
  val weatherDesc_schema: StructType = StructType(Array(
    StructField("datetime", DateType),
    StructField("Vancouver", StringType),
    StructField("Portland", StringType),
    StructField("San Francisco", StringType),
    StructField("Seattle", StringType),
    StructField("Los Angeles", StringType),
    StructField("San Diego", StringType),
    StructField("Las Vegas", StringType),
    StructField("Phoenix", StringType),
    StructField("Albuquerque", StringType),
    StructField("Denver", StringType),
    StructField("San Antonio", StringType),
    StructField("Dallas", StringType),
    StructField("Houston", StringType),
    StructField("Kansas City", StringType),
    StructField("Minneapolis", StringType),
    StructField("Saint Louis", StringType),
    StructField("Chicago", StringType),
    StructField("Nashville", StringType),
    StructField("Indianapolis", StringType),
    StructField("Atlanta", StringType),
    StructField("Detroit", StringType),
    StructField("Jacksonville", StringType),
    StructField("Charlotte", StringType),
    StructField("Miami", StringType),
    StructField("Pittsburgh", StringType),
    StructField("Toronto", StringType),
    StructField("Philadelphia", StringType),
    StructField("New York", StringType),
    StructField("Montreal", StringType),
    StructField("Boston", StringType),
    StructField("Beersheba", StringType),
    StructField("Tel Aviv District", StringType),
    StructField("Eilat", StringType),
    StructField("Haifa", StringType),
    StructField("Nahariyya", StringType),
    StructField("Jerusalem", StringType)
  ))
  val city_schema: StructType = StructType(Array(
    StructField("City", StringType),
    StructField("Country", StringType)
  ))



}
