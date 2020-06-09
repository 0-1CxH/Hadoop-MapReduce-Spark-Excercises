mvn package
# Please change the $AREAWATER_PATH to where the file is
AREAWATER_PATH="Path/to/file"
hadoop jar target/qhu027_lab2-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.App file:///$AREAWATER_PATH/AREAWATER.csv file:///$AREAWATER_PATH/copy_1.csv
cp $AREAWATER_PATH/AREAWATER.csv copy_2.csv
hadoop jar target/qhu027_lab2-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.App file:///$AREAWATER_PATH/AREAWATER.csv hdfs:///copy_3.csv
hadoop jar target/qhu027_lab2-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.App hdfs:///copy_3.csv hdfs:///copy_4.csv
hadoop jar target/qhu027_lab2-1.0-SNAPSHOT.jar  edu.ucr.cs.cs167.qhu027.App hdfs:///copy_4.csv file:///$AREAWATER_PATH/copy_5.csv
