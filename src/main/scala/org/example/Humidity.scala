package org.example

//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StructField, StructType}
import org.example.App.{humidity_schema, spark}

object Humidity extends App {

//  creating data frame
val manHumiditySchema_df = spark.read
  .format("CSV")
  .option("header", "true")
  .schema(humidity_schema)
  .load("src/main/resources/Dataset/humidity.csv")

  manHumiditySchema_df.printSchema()
  manHumiditySchema_df.show()

//  custom query
  manHumiditySchema_df.selectExpr("year(datetime) AS year", "*").groupBy("year").avg("Vancouver","Portland").show(5)
}
