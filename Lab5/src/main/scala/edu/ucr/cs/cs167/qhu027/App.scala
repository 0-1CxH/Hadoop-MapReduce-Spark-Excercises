package edu.ucr.cs.cs167.qhu027

/**
 * @author ${user.name}
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args : Array[String]) {
    val command: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    conf.setAppName("lab5")
    val sparkContext = new SparkContext(conf)
    try {
      val inputRDD: RDD[String] = sparkContext.textFile(inputfile)
      // Parse the input file using the tab separator and skip the first line
      val splitRDD = inputRDD.map(_.split("\t")).mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
      //Start timing
      val t1 = System.nanoTime
      command match {
        case "count-all" =>    // count total number of records in the file
          println(s"Total count for file '${inputfile}' is ${splitRDD.count()}")

        case "code-filter" => // Filter the file by response code, args(2), and print the total number of matching lines
          val res_code = args(2)
          println(s"Total count for file '${inputfile}' with response code ${res_code} is ${splitRDD.filter(t=>t(5)==res_code).count()}")

        case "time-filter" => // Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
          val from_time = args(2).toInt
          val to_time = args(3).toInt
          println(s"Total count for file '${inputfile}' in time range [${from_time}, ${to_time}] is " +
            s"${splitRDD.filter(t=>(t(2).toInt >= from_time)&&(t(2).toInt <= to_time) ).count()}")

        case "count-by-code" => // Group the lines by response code and count the number of records per group
          println(s"Number of lines per code for the file ${inputfile}")
          println("Code,Count")
          println(splitRDD.map(t=>(t(5),1)).countByKey().map(t=>s"${t._1},${t._2}").mkString("\n"))

        case "sum-bytes-by-code" => // Group the lines by response code and sum the total bytes per group
          println(s"Total bytes per code for the file ${inputfile}")
          println("Code,Sum(bytes)")
          splitRDD.map(t=>(t(5),t(6).toInt)).reduceByKey(_+_).collect().foreach(t=>println(s"${t._1},${t._2}"))

        case "avg-bytes-by-code" => // Group the liens by response code and calculate the average bytes per group
          println(s"Average bytes per code for the file ${inputfile}")
          println("Code,Avg(bytes)")
          splitRDD.map(t=>(t(5),(t(6).toFloat,1.0))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(t=>(t._1,t._2._1/t._2._2)).collect().foreach(t=>println(s"${t._1},${t._2}"))

        case "top-host" => // print the host the largest number of lines and print the number of lines
          println(s"Top host in the file ${inputfile} by number of entries")
          val th_res = splitRDD.map(t=>(t(0),1)).reduceByKey(_+_).sortBy(t=>t._2,false).first()
          println(s"Host: ${th_res._1}")
          println(s"Number of entries: ${th_res._2}")

        case "comparison" =>
        // Given a specific time, calculate the number of lines per response code for the
        // entries that happened before that time, and once more for the lines that happened at or after
        // that time. Print them side-by-side in a tabular form.
          val fil_time = args(2).toInt
          val upperRDD = splitRDD.filter(t=>(t(2).toInt >= fil_time))
          val upper_res = upperRDD.map(t=>(t(5),1)).countByKey()
          val lowerRDD = splitRDD.filter(t=>(t(2).toInt < fil_time))
          val lower_res = lowerRDD.map(t=>(t(5),1)).countByKey()//.map(t=>s"${t._1},${t._2}").mkString("\n")
          println(s"Comparison of the number of lines per code before and after ${fil_time} on file ${inputfile}")
          println("Code,Count before,Count after")
          upper_res.keys.foreach(t=>{
            if(lower_res.contains(t)) {println(s"${t},${lower_res(t)},${upper_res(t)}")}
            else {println(s"${t},0,${upper_res(t)}")}
          })
          lower_res.keys.foreach(t=>{
            if(upper_res.contains(t)) {}
            else {println(s"${t},${lower_res(t)},0")}
          })

      }
      val t2 = System.nanoTime
      println(s"Command '${command}' on file '${inputfile}' finished in ${(t2-t1)*1E-9} seconds")
    } finally {
      sparkContext.stop
    }
  }
}