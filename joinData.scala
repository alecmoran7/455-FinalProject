import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// IMPORTING THE DATASETS
val df_householdIncome = spark.read.options(Map("header"->"true")).csv("/fpDatasets/householdIncome.csv")
val df_dropout = spark.read.options(Map("header"->"true")).csv("/fpDatasets/svi_dropout.csv")
val df_internet = spark.read.options(Map("header"->"true")).csv("/fpDatasets/internetData.csv")
val df_codes = spark.read.options(Map("header"->"true")).csv("/fpDatasets/US_FIPS_Codes.csv")
val df_svi= spark.read.options(Map("header"->"true")).csv("/fpDatasets/se_svi.csv")
//------------------------

// REDUCING THE DATASETS
val internet_part_1 = df_internet.groupBy("STATE", "STATE_NUMBER", "COUNTY_NUMBER").agg(mean("MaxAdDown").alias("AVERAGE_DL_SPEED"))
val internet_part_2 = df_internet.groupBy("STATE", "STATE_NUMBER", "COUNTY_NUMBER").agg(mean("MaxAdUp").alias("AVERAGE_UP_SPEED"))
val df_internet_reduced = internet_part_2.join(internet_part_1, Seq("STATE", "STATE_NUMBER", "COUNTY_NUMBER"))
val df_dropout_reduced = df_dropout.groupBy("STATE","COUNTY").agg(mean("MP_NOHSDP").alias("RATIO_NO_HS_DIPLOMA"))
val df_income_reduced = df_householdIncome.groupBy("STATE","COUNTY").agg(mean("2010_MEDIAN_HOUSEHOLD_INCOME").alias("2010_AVERAGE_HOUSEHOLD_INCOME"))
val df_svi_reduced = df_svi.groupBy("STATE","COUNTY").agg(mean("RPL_THEME1").alias("SVI_SCORE_SOCIOECONOMIC"))
//---------------------

// JOINING THE DATASETS
val ft_1 = df_income_reduced.join(df_codes.withColumnRenamed("COUNTY NAME","COUNTY"), Seq("STATE","COUNTY"))
val ft_2 = ft_1.join(df_dropout_reduced, Seq("STATE", "COUNTY"))
val ft_3 = ft_2.join(df_svi_reduced, Seq("STATE", "COUNTY"))
val ft_4 = ft_3.join(df_internet_reduced.withColumnRenamed("STATE","STATE_ABBREV"), ft_3("FIPS STATE") === df_internet_reduced("STATE_NUMBER") && ft_3("FIPS COUNTY") === df_internet_reduced("COUNTY_NUMBER"), "outer")
val ft_5 = ft_4.drop("FIPS STATE").drop("FIPS COUNTY").drop("STATE_NUMBER").drop("COUNTY_NUMBER").drop("STATE_ABBREV")
ft_5.show(true)
ft_5.write.csv("/fpOutput/allData/")


