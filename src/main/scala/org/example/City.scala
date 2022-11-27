package org.example

import org.apache.spark.sql.functions.{col, expr, length, regexp_replace, split, substring}
import org.example.App.{city_schema, spark}

import java.io.{FileNotFoundException, IOException}

object City extends App {

  try {
    //    city_attributes
    val manCitySchema_df = spark.read
      .format("CSV")
      .option("header", "true")
      .schema(city_schema)
      .load("src/main/resources/Dataset/city_attributes.csv")

    manCitySchema_df.printSchema()
    manCitySchema_df.show()
  } catch {
    case ex1: FileNotFoundException => println(s"File Not Found")
    case ex2: IOException => println(s"Input/ Output parameter Missing")
    case ex3: IndexOutOfBoundsException => println(s"Index Out of bound")
    case ex4: IllegalArgumentException => println(s"Illegal Argument")
    case ex5: InterruptedException => println(s"Interrupted")
    case unknown: Exception => println(s"Not sure why the error is coming")
  }
//  val result = manCitySchema_df.withColumn("New Title", expr("substring(title, 1, length(title)-1)"))
//    .withColumn("Year", expr("substring(title, -6, 6)"))
//    .withColumn("year", regexp_replace(col("year"), "[\\(,\\)]", ""))
//    .withColumn("New_gener", split(col("gener"), "\\|"))
//    .select(col("*") +:(0 until 5).map(i  => col("New_gener").getItem(i).as(s"col$i"):_*))
//  result.write.format("csv").save("src/main/resources/Dataset/Output")

}
