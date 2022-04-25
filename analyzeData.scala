import org.apache.spark.sql.DataFrameStatFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Importing and parsing the dataset 
val df_imported= spark.read.options(Map("header"->"true")).csv("/fpDatasets/job_1_output_trimmed.csv")
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




