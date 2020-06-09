# Lab 7

## Student information
* Full name: Qicheng Hu
* E-mail: qhu027@cs.ucr.edu
* UCR NetID: qhu027
* Student ID: X675102

## Answers

* (Q1) What are the top five crime types?

  The top 5 crime types are theft, battery, criminal damage, narcotics and assault.
  
  
  
* (Q2) Compare the sizes of the CSV file and the resulting Parquet file? What do you notice? Explain.

  The CSV file is 1.68GB, the Parquet is only 457.1MB. Parquet is much smaller than CSV, I think it is because that block in Parquet is compressed.

  

* (Q3) How many times do you see the schema the output? How does this relate to the number of files in the output directory? What do you make of that?

  The schema part appears 13 times, as many times as the number of Parquet files in folder

  

* (Q4) How does the output look like? How many files were generated?

  In the output folder, therer are folders named "District="+(Different Values). There are 306 files in all folders.

  

* (Q5) Explain an efficient way to run this query on a column store.

  If stored in column format, the filter only needs to read the blocks belong to that column filter and record row number, then it gets the expected line to output.

  

* (Q6) Which of the three input files you think will be processed faster using this operation?

  The Parquet which is partitioned by district should has the fastest speed of processing. This operation uses data from only one district and the partitioned file saves time on filtering.



* (Running Time)

  |                 | CSV                | Parquet           | Parquet-Partitioned |
  | --------------- | ------------------ | ----------------- | ------------------- |
  | top-crime-types | 9.839406669        | 1.920732633       | 3.0499206780000003  |
  | find            | 15.072712987000001 | 5.175159292       | 3.001431841         |
  | stats           | 40.41209391        | 5.904573877000001 | 5.523294336         |
  | stats-district  | 21.282903724       | 4.959801898       | 3.6017020210000004  |
  |                 |                    |                   |                     |

  

