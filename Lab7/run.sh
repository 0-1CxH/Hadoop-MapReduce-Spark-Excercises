
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar write-parquet Crimes_-_2001_to_present.csv Crimes_-_2001_to_present.parquet 
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar write-parquet-partitioned Crimes_-_2001_to_present.csv Crimes_-_2001_to_present_partitioned.parquet

spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar Crimes_-_2001_to_present.csv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar find Crimes_-_2001_to_present.csv HY413923
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats Crimes_-_2001_to_present.csv
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats-district Crimes_-_2001_to_present.csv 8

spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar top-crime-types Crimes_-_2001_to_present.parquet
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar find Crimes_-_2001_to_present.parquet HY413923
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats Crimes_-_2001_to_present.parquet
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats-district Crimes_-_2001_to_present.parquet 8

spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar top-crime-types Crimes_-_2001_to_present_partitioned.parquet 
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar find Crimes_-_2001_to_present_partitioned.parquet HY413923
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats Crimes_-_2001_to_present_partitioned.parquet
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab7-1.0-SNAPSHOT.jar stats-district Crimes_-_2001_to_present_partitioned.parquet 8
