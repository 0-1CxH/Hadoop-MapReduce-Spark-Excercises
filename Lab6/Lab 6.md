./._output                                                                                          000644  000765  000024  00000000343 13656161522 012662  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2   �      �                                      ATTR       �   �                     �     com.apple.TextEncoding      �     com.apple.lastuseddate#PS    utf-8;134217984C�^    s7                                                                                                                                                                                                                                                                                                 output                                                                                              000644  000765  000024  00000005602 13656161522 012313  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         Using Spark master 'local[*]'
Total count for file 'nasa_19950630.22-19950728.12.tsv' is 1891709
Command 'count-all' on file 'nasa_19950630.22-19950728.12.tsv' finished in 0.693072582 seconds
Using Spark master 'local[*]'
Total count for file 'nasa_19950630.22-19950728.12.tsv' with response code 302 is 46573
Command 'code-filter' on file 'nasa_19950630.22-19950728.12.tsv' finished in 1.1949845700000001 seconds
Using Spark master 'local[*]'
Total count for file 'nasa_19950630.22-19950728.12.tsv' in time range [804955673, 805590159] is 554919
Command 'time-filter' on file 'nasa_19950630.22-19950728.12.tsv' finished in 1.34466729 seconds
Using Spark master 'local[*]'
Number of lines per code for the file nasa_19950630.22-19950728.12.tsv
+--------+-------+
|response|  count|
+--------+-------+
|     501|     14|
|     500|     62|
|     403|     54|
|     404|  10845|
|     200|1701534|
|     304| 132627|
|     302|  46573|
+--------+-------+

Command 'count-by-code' on file 'nasa_19950630.22-19950728.12.tsv' finished in 2.320827548 seconds
Using Spark master 'local[*]'
Total bytes per code for the file nasa_19950630.22-19950728.12.tsv
+--------+-----------+
|response| sum(bytes)|
+--------+-----------+
|     501|          0|
|     500|          0|
|     403|          0|
|     404|          0|
|     200|38692291442|
|     304|          0|
|     302|    3682049|
+--------+-----------+

Command 'sum-bytes-by-code' on file 'nasa_19950630.22-19950728.12.tsv' finished in 2.5073181850000004 seconds
Using Spark master 'local[*]'
Average bytes per code for the file nasa_19950630.22-19950728.12.tsv
+--------+------------------+
|response|        avg(bytes)|
+--------+------------------+
|     501|               0.0|
|     500|               0.0|
|     403|               0.0|
|     404|               0.0|
|     200|22739.652244386536|
|     304|               0.0|
|     302|  79.0597341807485|
+--------+------------------+

Command 'avg-bytes-by-code' on file 'nasa_19950630.22-19950728.12.tsv' finished in 2.496198102 seconds
Using Spark master 'local[*]'
Top host in the file nasa_19950630.22-19950728.12.tsv by number of entries
Host: piweba3y.prodigy.com
Number of entries: 17572
Command 'top-host' on file 'nasa_19950630.22-19950728.12.tsv' finished in 2.8730508930000003 seconds
Using Spark master 'local[*]'
Comparison of the number of lines per code before and after 805383872 on file nasa_19950630.22-19950728.12.tsv
+--------+------------+-----------+
|response|count-before|count-after|
+--------+------------+-----------+
|     501|           2|         12|
|     500|          53|          9|
|     403|          19|         35|
|     404|        3864|       6981|
|     200|      594412|    1107122|
|     304|       38000|      94623|
|     302|       21057|      25516|
+--------+------------+-----------+

Command 'comparison' on file 'nasa_19950630.22-19950728.12.tsv' finished in 3.6691426820000004 seconds
                                                                                                                              ./._pom.xml                                                                                         000644  000765  000024  00000000260 13656136545 012721  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2   ~      �                                      ATTR       �   �                     �     com.apple.lastuseddate#PS    �ŷ^    �y                                                                                                                                                                                                                                                                                                                                                    pom.xml                                                                                             000644  000765  000024  00000010144 13656136545 012351  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>edu.ucr.cs.cs167.qhu027</groupId>
  <artifactId>qhu027_lab6</artifactId>
  <version>1.0-SNAPSHOT</version>
  <name>${project.artifactId}</name>
  <description>My wonderfull scala app</description>
  <inceptionYear>2018</inceptionYear>
  <licenses>
    <license>
      <name>My License</name>
      <url>http://....</url>
      <distribution>repo</distribution>
    </license>
  </licenses>

  <properties>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <encoding>UTF-8</encoding>
    <scala.version>2.12.11</scala.version>
    <scala.compat.version>2.12</scala.compat.version>
    <spec2.version>4.2.0</spec2.version>
    <spark.version>2.4.5</spark.version>
  </properties>



  <dependencies>
    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>${scala.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.12</artifactId>
      <version>${spark.version}</version>
      <scope>compile</scope>
    </dependency>

    <!-- Test -->
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.12</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.scalatest</groupId>
      <artifactId>scalatest_${scala.compat.version}</artifactId>
      <version>3.0.5</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.specs2</groupId>
      <artifactId>specs2-core_${scala.compat.version}</artifactId>
      <version>${spec2.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.specs2</groupId>
      <artifactId>specs2-junit_${scala.compat.version}</artifactId>
      <version>${spec2.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_2.12</artifactId>
      <version>2.4.5</version>
    </dependency>
  </dependencies>


  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <testSourceDirectory>src/test/scala</testSourceDirectory>
    <plugins>
      <plugin>
        <!-- see http://davidb.github.com/scala-maven-plugin -->
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>3.3.2</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
            <configuration>
              <args>
                <arg>-dependencyfile</arg>
                <arg>${project.build.directory}/.scala_dependencies</arg>
              </args>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
          <!-- Tests will be run with scalatest-maven-plugin instead -->
          <skipTests>true</skipTests>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
        <version>2.0.0</version>
        <configuration>
          <reportsDirectory>${project.build.directory}/surefire-reports</reportsDirectory>
          <junitxml>.</junitxml>
          <filereports>TestSuiteReport.txt</filereports>
          <!-- Comma separated list of JUnit test class names to execute -->
          <jUnitClasses>samples.AppTest</jUnitClasses>
        </configuration>
        <executions>
          <execution>
            <id>test</id>
            <goals>
              <goal>test</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
                                                                                                                                                                                                                                                                                                                                                                                                                            ./._run.sh                                                                                          000644  000765  000024  00000000600 13656160553 012536  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                             Mac OS X            	   2  N     �                                      ATTR      �     x                      com.apple.TextEncoding          com.apple.lastuseddate#PS      '   Y  7com.apple.metadata:kMDLabel_3babd4oio7dzqb6cbaytg4ss64   utf-8;134217984��^    �    �a]Ÿ�]�H�<�LĦ<F���0m=>�%��8��#�MvGGhM���!�*c'�qi�`Z�Y��H�C�K[��ݵm����o��                                                                                                                                run.sh                                                                                              000644  000765  000024  00000002133 13656160553 012167  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         mvn package clean
mvn package

spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar count-all nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar code-filter nasa_19950630.22-19950728.12.tsv 302
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar time-filter nasa_19950630.22-19950728.12.tsv 804955673 805590159
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar count-by-code nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar sum-bytes-by-code nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar avg-bytes-by-code nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar top-host nasa_19950630.22-19950728.12.tsv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab6-1.0-SNAPSHOT.jar comparison nasa_19950630.22-19950728.12.tsv 805383872


                                                                                                                                                                                                                                                                                                                                                                                                                                     src/                                                                                                000755  000765  000024  00000000000 13655742314 011617  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/test/                                                                                           000755  000765  000024  00000000000 13655742314 012576  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/                                                                                           000755  000765  000024  00000000000 13655742314 012543  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/                                                                                     000755  000765  000024  00000000000 13655742314 013626  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/                                                                                 000755  000765  000024  00000000000 13655742314 014403  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/ucr/                                                                             000755  000765  000024  00000000000 13655742314 015174  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/ucr/cs/                                                                          000755  000765  000024  00000000000 13655742314 015601  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/ucr/cs/cs167/                                                                    000755  000765  000024  00000000000 13655742314 016444  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/ucr/cs/cs167/qhu027/                                                             000755  000765  000024  00000000000 13656160175 017472  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/main/scala/edu/ucr/cs/cs167/qhu027/App.scala                                                    000644  000765  000024  00000007331 13656160175 021223  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         package edu.ucr.cs.cs167.qhu027

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
}                                                                                                                                                                                                                                                                                                       src/test/scala/                                                                                     000755  000765  000024  00000000000 13655742314 013661  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/test/scala/samples/                                                                             000755  000765  000024  00000000000 13655742314 015325  5                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         src/test/scala/samples/specs.scala                                                                  000644  000765  000024  00000001414 13655742314 017447  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         package samples

import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner._
  

/**
 * Sample specification.
 * 
 * This specification can be executed with: scala -cp <your classpath=""> ${package}.SpecsTest
 * Or using maven: mvn test
 *
 * For more information on how to write or run specifications, please visit: 
 *   http://etorreborre.github.com/specs2/guide/org.specs2.guide.Runners.html
 *
 */
@RunWith(classOf[JUnitRunner])
class MySpecTest extends Specification {
  "The 'Hello world' string" should {
    "contain 11 characters" in {
      "Hello world" must have size(11)
    }
    "start with 'Hello'" in {
      "Hello world" must startWith("Hello")
    }
    "end with 'world'" in {
      "Hello world" must endWith("world")
    }
  }
}
                                                                                                                                                                                                                                                    src/test/scala/samples/junit.scala                                                                  000644  000765  000024  00000000263 13655742314 017464  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         package samples

import org.junit._
import Assert._

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(true)

//    @Test
//    def testKO() = assertTrue(false)

}


                                                                                                                                                                                                                                                                                                                                             src/test/scala/samples/scalatest.scala                                                              000644  000765  000024  00000006022 13655742314 020315  0                                                                                                    ustar 00qhu                             staff                           000000  000000                                                                                                                                                                         /*
 * Copyright 2001-2009 Artima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package samples

/*
ScalaTest facilitates different styles of testing by providing traits you can mix
together to get the behavior and syntax you prefer.  A few examples are
included here.  For more information, visit:

http://www.scalatest.org/

One way to use ScalaTest is to help make JUnit or TestNG tests more
clear and concise. Here's an example:
*/
import scala.collection._
import org.scalatest.Assertions
import org.junit.Test

class StackSuite extends Assertions {

  @Test def stackShouldPopValuesIinLastInFirstOutOrder() {
    val stack = new mutable.ArrayStack[Int]
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

  @Test def stackShouldThrowRuntimeExceptionIfAnEmptyArrayStackIsPopped() {
    val emptyStack = new mutable.ArrayStack[String]
    intercept[RuntimeException] {
      emptyStack.pop()
    }
  }
}

/*
Here's an example of a FunSuite with Matchers mixed in:
*/
import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class ListSuite extends FunSuite with Matchers {

  test("An empty list should be empty") {
    List() should be ('empty)
    Nil should be ('empty)
  }

  test("A non-empty list should not be empty") {
    List(1, 2, 3) should not be ('empty)
    List("fee", "fie", "foe", "fum") should not be ('empty)
  }

  test("A list's length should equal the number of elements it contains") {
    List() should have length (0)
    List(1, 2) should have length (2)
    List("fee", "fie", "foe", "fum") should have length (4)
  }
}

/*
ScalaTest also supports the behavior-driven development style, in which you
combine tests with text that specifies the behavior being tested. Here's
an example whose text output when run looks like:

A Map
- should only contain keys and values that were added to it
- should report its size as the number of key/value pairs it contains
*/
import org.scalatest.FunSpec

class ExampleSpec extends FunSpec {

  describe("An ArrayStack") {

    it("should pop values in last-in-first-out order") {
      val stack = new mutable.ArrayStack[Int]
      stack.push(1)
      stack.push(2)
      assert(stack.pop() === 2)
      assert(stack.pop() === 1)
    }

    it("should throw RuntimeException if an empty array stack is popped") {
      val emptyStack = new mutable.ArrayStack[Int]
      intercept[RuntimeException] {
        emptyStack.pop()
      }
    }
  }
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              