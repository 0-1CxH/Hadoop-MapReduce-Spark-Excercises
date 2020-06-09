mvn package clean
mvn package

spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab9-1.0-SNAPSHOT.jar kc_house_data.csv regression
spark-submit --class edu.ucr.cs.cs167.qhu027.App target/qhu027_lab9-1.0-SNAPSHOT.jar sentiment.csv classification