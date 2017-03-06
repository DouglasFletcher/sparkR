
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-avro_2.11:3.0.0" "sparkr-shell"')

# add library location for SparkR
.libPaths(
	c(file.path(Sys.getenv("R_LIBS_USER"))
	, file.path(Sys.getenv("HADOOP_HOME"))
	, file.path(Sys.getenv("SPARK_HOME"), "R", "lib")
	, .libPaths())
)
