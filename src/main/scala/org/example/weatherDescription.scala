package org.example

import org.example.App.{spark, weatherDesc_schema}

object weatherDescription extends App {
  //    weather_description
  val manWeatherDescSchema_df = spark.read
    .format("CSV")
    .option("header", "true")
    .schema(weatherDesc_schema)
    .load("src/main/resources/Dataset/weather_description.csv")

      manWeatherDescSchema_df.printSchema()
      manWeatherDescSchema_df.show()


}
