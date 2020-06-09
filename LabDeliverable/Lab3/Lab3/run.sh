mvn package
# Please change $WORK_DIR to where the sample file is
WORK_DIR="Users/qhu/Documents/167/Projects/qhu027_lab3"
SAMPLE_FILENAME="nasa_19950801.tsv"
yarn jar target/qhu027_lab3-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.Filter file:///$WORK_DIR/$SAMPLE_FILENAME file:///$WORK_DIR/filter_output  "200"
yarn jar target/qhu027_lab3-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.Aggregate file:///$WORK_DIR/filter_output/part-m-00000 file:///$WORK_DIR/aggregation_output
 
