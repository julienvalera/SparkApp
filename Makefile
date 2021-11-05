spark-shell:
	spark-shell \
	--packages \
	org.apache.hadoop:hadoop-aws:3.3.1,\
	net.snowflake:snowflake-jdbc:3.13.6,\
	net.snowflake:spark-snowflake_2.12:2.9.1-spark_3

spark-submit:
	spark-submit \
	--class main.scala.SparkApp \
	--master local \
	--packages \
	org.apache.hadoop:hadoop-aws:3.3.1,\
	net.snowflake:snowflake-jdbc:3.13.6,\
	net.snowflake:spark-snowflake_2.12:2.9.1-spark_3.1 \
	target/scala-2.12/sparkapp_2.12-0.1.jar