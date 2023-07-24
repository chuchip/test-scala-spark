package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}



object SparkDemo{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("test-spark").getOrCreate()
    println("Printing spark session")
    println("App Name" + spark.sparkContext.appName)
    println("Deployment mode"+spark.sparkContext.deployMode)
    println("Master "+spark.sparkContext.master)
    println("-----")
    if (args.length <= 0) {
      println("usage apache-spark-demo <file path to blogs.json")
      System.exit(1)
    }
    //get the path to the JSON file
    val jsonFile = args(0)
    //define our schema as before
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    //Create a DataFrame by reading from the JSON file a predefined Schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    //show the DataFrame schema as output
    blogsDF.show(truncate = false)
    // print the schemas
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    // Show columns and expressions
    blogsDF.select(expr("Hits") * 2).show(2)
    blogsDF.select(col("Hits") * 2).show(2)
    blogsDF.select(expr("Hits * 2")).show(2)
    // show heavy hitters
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    spark.stop()
  }
}