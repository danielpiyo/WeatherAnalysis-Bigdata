package org.example

import org.example.App.{spark, temperature_schema}


object Temperature extends App {


//  //    creating dataframes
//  var temperature_df = spark.read.format("CSV").load("src/main/resources/Dataset/temperature.csv")
//      temperature_df.printSchema()
//      temperature_df.show()

//  temperature dataframe
  val manTemperatureSchema_df = spark.read
    .format("CSV")
    .option("header", "true")
    .schema(temperature_schema)
    .load("src/main/resources/Dataset/temperature.csv")

      manTemperatureSchema_df.printSchema()
      manTemperatureSchema_df.show()

//  Custom query
  manTemperatureSchema_df.selectExpr("year(datetime) AS year", "*")
    .groupBy("year")
    .min("Vancouver","Portland", "San Francisco",
      "Seattle", "Los Angeles", "San Diego",
      "Las Vegas", "Phoenix", "Albuquerque",
      "Denver", "San Antonio", "Dallas", "Houston",
      "Kansas City", "Minneapolis", "Saint Louis",
      "Chicago", "Nashville", "Indianapolis",
      "Atlanta", "Detroit", "Jacksonville", "Charlotte",
      "Miami", "Pittsburgh", "Toronto", "Philadelphia",
      "New York", "Montreal", "Boston", "Beersheba",
      "Tel Aviv District", "Eilat", "Haifa","Nahariyya", "Jerusalem")
    .where("year IS NOT NULL")
    .limit(5)
    .show(truncate=false)

  //  max temperature grouped by year
  manTemperatureSchema_df.selectExpr("year(datetime) AS year", "month(datetime) as month", "*")
    .groupBy("year", "month")
    .max("Vancouver","Portland", "San Francisco",
      "Seattle", "Los Angeles", "San Diego",
      "Las Vegas", "Phoenix", "Albuquerque",
      "Denver", "San Antonio", "Dallas", "Houston",
      "Kansas City", "Minneapolis", "Saint Louis",
      "Chicago", "Nashville", "Indianapolis",
      "Atlanta", "Detroit", "Jacksonville", "Charlotte",
      "Miami", "Pittsburgh", "Toronto", "Philadelphia",
      "New York", "Montreal", "Boston", "Beersheba",
      "Tel Aviv District", "Eilat", "Haifa","Nahariyya", "Jerusalem")
    .where("year IS NOT NULL")
    .write.format("csv").save("s3a://capstonproject01/MaxTemperature/Output/")

}
