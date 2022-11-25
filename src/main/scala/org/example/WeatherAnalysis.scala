package org.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, StructField, StructType}
object WeatherAnalysis extends App{

//  creating spark session
    val spark = SparkSession.builder()
      .appName("WeatherAnalysis01")
      .master("local")
      .getOrCreate()

//    creating dataframes
    var temperature_df = spark.read.format("CSV").load("src/main/resources/Dataset/temperature.csv")
    temperature_df.printSchema()
    temperature_df.show()

//    creating schema
//    val temperature_schema = StructType(Array(
//        StructField("datetime", DateType)
//    ))
}
