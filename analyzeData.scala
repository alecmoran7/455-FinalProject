import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val fs = FileSystem.get(sc.hadoopConfiguration)
val file = fs.globStatus(new Path("/fpOutput/job1/part*"))(0).getPath().toString()
val df_imported= spark.read.options(Map("header"->"true")).csv(file)
val df_main= df_imported.selectExpr("STATE","COUNTY","cast(2010_AVERAGE_HOUSEHOLD_INCOME as double) 2010_AVERAGE_HOUSEHOLD_INCOME","cast(RATIO_NO_HS_DIPLOMA as double) RATIO_NO_HS_DIPLOMA","cast(SVI_SCORE_SOCIOECONOMIC as double) SVI_SCORE_SOCIOECONOMIC","cast(AVERAGE_UP_SPEED as double) AVERAGE_UP_SPEED","cast(AVERAGE_DL_SPEED as double) AVERAGE_DL_SPEED")
df_main.printSchema()
  //------------------------

// Computing covariance
print("Covariance between internet download speed and the number of high school dropouts per county:" + df_main.stat.cov("AVERAGE_DL_SPEED", "RATIO_NO_HS_DIPLOMA"))
print("Covariance between internet upload speed and the number of high school dropouts per county:" + df_main.stat.cov("AVERAGE_UP_SPEED", "RATIO_NO_HS_DIPLOMA"))
print("Covariance between number of high school dropouts and socioeconomic vulnerability levels per county:" + df_main.stat.cov("RATIO_NO_HS_DIPLOMA", "SVI_SCORE_SOCIOECONOMIC"))
print("Covariance between number of high school dropouts and household income levels per county:" + df_main.stat.cov("RATIO_NO_HS_DIPLOMA", "2010_AVERAGE_HOUSEHOLD_INCOME"))
print("Covariance between internet download speed and socioeconomic vulnerability per county:" + df_main.stat.cov("AVERAGE_DL_SPEED", "SVI_SCORE_SOCIOECONOMIC"))
print("Covariance between internet upload speed and socioeconomic vulnerability per county:" + df_main.stat.cov("AVERAGE_UP_SPEED", "SVI_SCORE_SOCIOECONOMIC"))
//------------------------

// Computing correlation coefficient
print("[MAIN INTEREST]: Correlation between internet download speed and the number of high school dropouts per county:" + df_main.stat.corr("AVERAGE_DL_SPEED", "RATIO_NO_HS_DIPLOMA"))
print("[MAIN INTEREST]: Correlation between internet upload speed and the number of high school dropouts per county:" + df_main.stat.corr("AVERAGE_UP_SPEED", "RATIO_NO_HS_DIPLOMA"))
print("Correlation between number of high school dropouts and socioeconomic vulnerability levels per county:" + df_main.stat.corr("RATIO_NO_HS_DIPLOMA", "SVI_SCORE_SOCIOECONOMIC"))
print("Correlation between number of high school dropouts and household income levels per county:" + df_main.stat.corr("RATIO_NO_HS_DIPLOMA", "2010_AVERAGE_HOUSEHOLD_INCOME"))
print("Correlation between internet download speed and socioeconomic vulnerability per county:" + df_main.stat.corr("AVERAGE_DL_SPEED", "SVI_SCORE_SOCIOECONOMIC"))
print("Correlation between internet upload speed and socioeconomic vulnerability per county:" + df_main.stat.corr("AVERAGE_UP_SPEED", "SVI_SCORE_SOCIOECONOMIC"))
//------------------------

var assembler = new VectorAssembler().setInputCols(Array("2010_AVERAGE_HOUSEHOLD_INCOME","SVI_SCORE_SOCIOECONOMIC","AVERAGE_UP_SPEED","AVERAGE_DL_SPEED")).setOutputCol("features")
var df = assembler.transform(df_main)

val df_1 = df.drop("STATE")                                                                                                                                                  
val df_2 = df_1.drop("COUNTY")                                                                                                                                               
val df_3 = df_2.drop("2010_AVERAGE_HOUSEHOLD_INCOME")                                                                                                      
val df_4 = df_3.drop("SVI_SCORE_SOCIOECONOMIC")                                                                                                                              
val df_5 = df_4.drop("AVERAGE_UP_SPEED")                                                                                                                                     
val df_6 = df_5.drop("AVERAGE_DL_SPEED")

val df_7 = df_6.withColumnRenamed("RATIO_NO_HS_DIPLOMA","label")

var lr = new LinearRegression()
var Array(train, test) = df_7.randomSplit(Array(.8,.2), 42)
var lrModel = lr.fit(train)
var lrPredictions = lrModel.transform(test)
var re = new RegressionEvaluator()
re.setMetricName("rmse")
val rmse = re.evaluate(lrPredictions)
re.setMetricName("mse")
val mse = re.evaluate(lrPredictions)
re.setMetricName("r2")
val r2 = re.evaluate(lrPredictions)
re.setMetricName("mae")
val mae = re.evaluate(lrPredictions)

