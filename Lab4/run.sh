mvn package

spark-submit --class edu.ucr.cs.cs167.qhu027.App --master local[2] target/qhu027_lab4-1.0-SNAPSHOT.jar nasa_19950801.tsv

spark-submit --class edu.ucr.cs.cs167.qhu027.Filter --master local[2] target/qhu027_lab4-1.0-SNAPSHOT.jar nasa_19950801.tsv filter_output 200

spark-submit --class edu.ucr.cs.cs167.qhu027.Aggregation --master local[2] target/qhu027_lab4-1.0-SNAPSHOT.jar nasa_19950801.tsv aggregation_output
