# 455-FinalProject


## Setup

Create a folder for the datasets and the output files using the following commands:

$ (HADOOP_HOME) -mkdir /fpDatasets
$ (HADOOP_HOME) -mkdir /fpOutput

Upload the files to the /fpDatasets hdfs folder using the commands:

$ (HADOOP_HOME) -put ./datasets/householdIncome.csv /fpDatasets/
$ (HADOOP_HOME) -put ./datasets/internetData.csv /fpDatasets/
$ (HADOOP_HOME) -put ./datasets/se_svi.csv /fpDatasets/
$ (HADOOP_HOME) -put ./datasets/svi_dropout.csv /fpDatasets/
$ (HADOOP_HOME) -put ./datasets/US_FIPS_Codes.csv /fpDatasets/


## Running the jobs:

Running job 1
$ /path/to/sparkshell < joinData.scala

Running job 2
$ /path/to/sparkshell < analyzeData.scala

## Reading the output
