package edu.ucr.cs.cs167.qhu027

/**
 * @author ${user.name}
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object App {

  def main(args : Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val command: String = args(0)
    val inputfile: String = args(1)

    val spark = SparkSession
      .builder()
      .appName("CS167 Lab6")
      .config(conf)
      .getOrCreate()

    try {
      val input = spark.read.format("csv")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputfile)

      import spark.implicits._

      //input.show()

      //input.printSchema()

      val t1 = System.nanoTime
      command match {
        case "count-all" =>
          println(s"Total count for file '${inputfile}' is ${input.count()}")
        // TODO count total number of records in the file

        case "code-filter" =>
        // TODO Filter the file by response code, args(2), and print the total number of matching lines
          val res_code = args(2)
          println(s"Total count for file '${inputfile}' with response code ${res_code} is ${input.filter($"response"===res_code).count()}")

        case "time-filter" =>
        // TODO Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
          val from_time = args(2).toInt
          val to_time = args(3).toInt
          println(s"Total count for file '${inputfile}' in time range [${from_time}, ${to_time}] is " +
            s"${input.filter($"time".between(from_time,to_time)).count()}")

        case "count-by-code" =>
        // TODO Group the lines by response code and count the number of records per group
          println(s"Number of lines per code for the file ${inputfile}")
          input.groupBy($"response").count().show()

        case "sum-bytes-by-code" =>
        // TODO Group the lines by response code and sum the total bytes per group
          println(s"Total bytes per code for the file ${inputfile}")
          input.groupBy("response").sum("bytes").show()

        case "avg-bytes-by-code" =>
        // TODO Group the liens by response code and calculate the average bytes per group
          println(s"Average bytes per code for the file ${inputfile}")
          input.groupBy("response").avg("bytes").show()

        case "top-host" =>
        // TODO print the host the largest number of lines and print the number of lines
          println(s"Top host in the file ${inputfile} by number of entries")
          val th_res = input.groupBy("host").count().orderBy($"count".desc).first()
          println(s"Host: ${th_res(0)}")
          println(s"Number of entries: ${th_res(1)}")

        case "comparison" =>
        // TODO Given a specific time, calculate the number of lines per response code for the
        // entries that happened before that time, and once more for the lines that happened at or after
        // that time. Print them side-by-side in a tabular form.
          val fil_time = args(2).toInt
          println(s"Comparison of the number of lines per code before and after ${fil_time} on file ${inputfile}")
          val upperPart = input.filter($"time">fil_time).groupBy("response").count().withColumnRenamed("count","count-after")
          val lowerPart = input.filter($"time"<fil_time).groupBy("response").count().withColumnRenamed("count","count-before")
          lowerPart.join(upperPart, "response").show()

      }
      val t2 = System.nanoTime
      println(s"Command '${command}' on file '${inputfile}' finished in ${(t2-t1)*1E-9} seconds")


    } finally {
      spark.stop
    }
  }
}