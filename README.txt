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


## Datasets:
US FIPS Codes: https://raw.githubusercontent.com/alecmoran7/455-FinalProject/main/datasets/US_FIPS_Codes.csv
Household Income: https://raw.githubusercontent.com/alecmoran7/455-FinalProject/main/datasets/householdIncome.csv
Socioeconomic Social Vulnerability Index: https://raw.githubusercontent.com/alecmoran7/455-FinalProject/main/datasets/se_svi.csv
SVI Dropout: https://raw.githubusercontent.com/alecmoran7/455-FinalProject/main/datasets/svi_dropout.csv
Internet Data: https://www.fcc.gov/general/broadband-deployment-data-fcc-form-477
  OR 
  https://github.com/alecmoran7/455-FinalProject/blob/main/datasets/internetData.csv
  oid sha256:95b668bb37ce928746472048dec55daefd515c6cab0ad0e1ea7a4e6ddf438b56
