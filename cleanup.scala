import sys.process._

//delete old data from hdfs
"hadoop fs -rm -r -f /tmp/BasketballStatsWithYear/"!
"hadoop fs -rm -r -f /tmp/BasketballStats/"!

spark.sql("DROP TABLE IF EXISTS basketball.players")
spark.sql("DROP TABLE IF EXISTS basketball.age")
spark.sql("DROP TABLE IF EXISTS basketball.experience")
spark.sql("DROP TABLE IF EXISTS basketball.similar")
spark.sql("DROP TABLE IF EXISTS basketball.ml")
spark.sql("DROP TABLE IF EXISTS basketball.regression_total")

