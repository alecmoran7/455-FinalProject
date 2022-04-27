# 455-FinalProject


## Setup

Create a folder for the datasets and the output files using the following commands:

$HADOOP_HOME/bin/hadoop fs -mkdir /fpDatasets 
$HADOOP_HOME/bin/hadoop fs -mkdir /fpOutput 

Upload the files to the /fpDatasets hdfs folder using the commands:

$HADOOP_HOME/bin/hadoop fs -put ./datasets/householdIncome.csv /fpDatasets/ 
$HADOOP_HOME/bin/hadoop fs -put ./datasets/internetData.csv /fpDatasets/ 
$HADOOP_HOME/bin/hadoop fs -put ./datasets/se_svi.csv /fpDatasets/ 
$HADOOP_HOME/bin/hadoop fs -put ./datasets/svi_dropout.csv /fpDatasets/ 
$HADOOP_HOME/bin/hadoop fs -put ./datasets/US_FIPS_Codes.csv /fpDatasets/ 


## Running the jobs:

Running job 1
$ /path/to/sparkshell < joinData.scala

Running job 2
$ /path/to/sparkshell < analyzeData.scala

