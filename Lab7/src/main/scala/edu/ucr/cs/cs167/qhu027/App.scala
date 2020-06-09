package edu.ucr.cs.cs167.qhu027

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

object App {

  def main(args : Array[String]) {
    val operation: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Lab7")
      .config(conf)
      .getOrCreate()

    try {
      import spark.implicits._
      // TODO Load input

      if (inputfile.slice(inputfile.length()-4,inputfile.length())==".csv") {
         var input = spark.read.format("csv")
          .option("sep", ",")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(inputfile)

        input = input.withColumnRenamed("Case Number", "Case_Number")
           .withColumnRenamed("Primary Type", "Primary_Type")
           .withColumnRenamed("Location Description", "Location_Description")
           .withColumnRenamed("Community Area", "Community_Area")
           .withColumnRenamed("FBI Code", "FBI_Code")
           .withColumnRenamed("X Coordinate", "X_Coordinate")
           .withColumnRenamed("Y Coordinate", "Y_Coordinate")
           .withColumnRenamed("Updated On", "Updated_On")
        //input.show()
        //input.printSchema()

        val t1 = System.nanoTime
        operation match {
          case "write-parquet" =>
            // TODO Write the input dataset to a parquet file. The file name is passed in args(2)
            input.write.parquet(args(2))
          case "write-parquet-partitioned" =>
          // TODO Write the input dataset to a partitioned parquet file by District. The file name is passed in args(2)
            input.write.partitionBy("District").parquet(args(2))
          case "top-crime-types" =>
          // TODO Print out the top five crime types by count "Primary_Type"
            input.groupBy("Primary_Type").count().orderBy($"count".desc).show(5)
          case "find" =>
          // TODO Find a record by Case_Number in args(2)
            input.filter($"Case_Number"===args(2)).show()
          case "stats" =>
          // TODO Compute the number of arrests, domestic crimes, and average beat per district.
          // Save the output to a new parquet file named "stats.parquet"

            val total_arrest = input.filter("Arrest==true").groupBy("District").count().withColumnRenamed("count","count_Arrest")
            val total_domestic = input.filter("Domestic==true").groupBy("District").count().withColumnRenamed("count","count_Domestic")
            val average_beat = input.groupBy("District").avg("Beat").withColumnRenamed("avg(Beat)","avg_Beat")
            val res = total_arrest.join(total_domestic, Seq("District"), "inner")
              .join(average_beat,  Seq("District"), "inner")
            res.write.mode("overwrite").parquet("stats.parquet")

          case "stats-district"  =>
          // TODO Compute number of arrests, domestic crimes, and average beat for one district (args(2))
          // Write the result to the standard output
            val total_arrest = input.filter($"District"===args(2)).filter("Arrest==true").groupBy("District").count().withColumnRenamed("count","count_Arrest")
            val total_domestic = input.filter($"District"===args(2)).filter("Domestic==true").groupBy("District").count().withColumnRenamed("count","count_Domestic")
            val average_beat = input.filter($"District"===args(2)).groupBy("District").avg("Beat").withColumnRenamed("avg(Beat)","avg_Beat")
            val res = total_arrest.join(total_domestic, Seq("District"), "inner")
              .join(average_beat,  Seq("District"), "inner")
            res.show()


        }

        val t2 = System.nanoTime
        println(s"Operation $operation on file '$inputfile' finished in ${(t2-t1)*1E-9} seconds")




      }
      else if (inputfile.slice(inputfile.length()-8,inputfile.length())==".parquet")
        {
          val input =  spark.read.parquet(inputfile)
          //input.show()
          //input.printSchema()
          val t1 = System.nanoTime
          operation match {
            case "top-crime-types" =>
              // TODO Print out the top five crime types by count "Primary_Type"
              input.groupBy("Primary_Type").count().orderBy($"count".desc).show(5)
            case "find" =>
              // TODO Find a record by Case_Number in args(2)
              input.filter($"Case_Number"===args(2)).show()
            case "stats" =>
              // TODO Compute the number of arrests, domestic crimes, and average beat per district.
              // Save the output to a new parquet file named "stats.parquet"

              val total_arrest = input.filter("Arrest==true").groupBy("District").count().withColumnRenamed("count","count_Arrest")
              val total_domestic = input.filter("Domestic==true").groupBy("District").count().withColumnRenamed("count","count_Domestic")
              val average_beat = input.groupBy("District").avg("Beat").withColumnRenamed("avg(Beat)","avg_Beat")
              val res = total_arrest.join(total_domestic, Seq("District"), "inner")
                .join(average_beat,  Seq("District"), "inner")
              res.write.mode("overwrite").parquet("stats.parquet")

            case "stats-district"  =>
              // TODO Compute number of arrests, domestic crimes, and average beat for one district (args(2))
              // Write the result to the standard output
              val total_arrest = input.filter($"District"===args(2)).filter("Arrest==true").groupBy("District").count().withColumnRenamed("count","count_Arrest")
              val total_domestic = input.filter($"District"===args(2)).filter("Domestic==true").groupBy("District").count().withColumnRenamed("count","count_Domestic")
              val average_beat = input.filter($"District"===args(2)).groupBy("District").avg("Beat").withColumnRenamed("avg(Beat)","avg_Beat")
              val res = total_arrest.join(total_domestic, Seq("District"), "inner")
                .join(average_beat,  Seq("District"), "inner")
              res.show()
          }

          val t2 = System.nanoTime
          println(s"Operation $operation on file '$inputfile' finished in ${(t2-t1)*1E-9} seconds")

        }





    } finally {
      spark.stop
    }
  }
}