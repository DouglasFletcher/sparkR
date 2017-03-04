
# set spark home location
#Sys.setenv(SPARK_HOME = "C:/spark-2.0.1-bin-hadoop2.7")
Sys.setenv(SPARK_HOME = "C:/spark-2.1.0-bin-hadoop2.7")
Sys.setenv(HADOOP_HOME = "C:/winutils")
Sys.setenv(R_LIBS_USER = "C:/Users/douglas.fletcher/Documents/R/win-library/3.3")

# additional packages
#Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.11:1.2.0" "sparkr-shell"')
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-avro_2.11:3.0.0" "sparkr-shell"')

Sys.setenv(R_LIBS_SITE=Sys.getenv("R_LIBS_USER"))

# add library location for SparkR
.libPaths(
	c(file.path(Sys.getenv("R_LIBS_USER"))
	, file.path(Sys.getenv("HADOOP_HOME"))
	, file.path(Sys.getenv("SPARK_HOME"), "R", "lib")
	, .libPaths())
)
