# cs346-big-data-coursework

## Commands
```
# start the hadoop service
$HADOOP_HOME/sbin/start-all.sh

# stop all services
$HADOOP_HOME/sbin/stop-all.sh

# check that all daemons are up and running
jps

# turning off safe mode
hdfs dfsadmin -safemode leave

# running query 1 java map reduce 
bin/hadoop jar your_package.jar main K start_date end_date input_file output_directory
```