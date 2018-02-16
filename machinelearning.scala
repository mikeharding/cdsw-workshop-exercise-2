val dbName = "basketball"

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer



//**********
//Regression
//**********

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics


val statArray = Array("zfg","zft")

for (stat <- statArray){

  val stat0=stat +"0"
  //set up vector with features
  val features = Array("exp", stat+"0")
  val assembler = new VectorAssembler()
  assembler.setInputCols(features)
  assembler.setOutputCol("features")

  //linear regression
  val lr = new LinearRegression()

  //set up parameters
  val builder = new ParamGridBuilder()
  builder.addGrid(lr.regParam, Array(0.1, 0.01, 0.001))
  builder.addGrid(lr.fitIntercept)
  builder.addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
  val paramGrid = builder.build()

  //define pipline
  val pipeline = new Pipeline()
  pipeline.setStages(Array(assembler, lr))

  //set up tvs
  val tvs = new TrainValidationSplit()
  tvs.setEstimator(pipeline)
  tvs.setEvaluator(new RegressionEvaluator)
  tvs.setEstimatorParamMaps(paramGrid)
  tvs.setTrainRatio(0.75)

  //define train and test data
  val trainData = spark.sql(s"select name, year, exp, mp, $stat0, $stat as label from $dbName.ml where year >= 2017 and year<2018")
  val testData = spark.sql(s"select name, year, exp, mp, $stat0, $stat  as label from $dbName.ml where year=2018")

  //create model
  val model = tvs.fit(trainData)

  //create predictions
  val predictions = model.transform(testData).select("name", "year", "prediction","label")

  //Get RMSE
  val rm = new RegressionMetrics(predictions.rdd.map(x => (x(2).asInstanceOf[Double], x(3).asInstanceOf[Double])))
  //println("Mean Squared Error " + stat + " : " + rm.meanSquaredError)
  println("Root Mean Squared Error " + stat + " : " + rm.rootMeanSquaredError)

//save as temp table
  predictions.registerTempTable(stat + "_temp")

}

//add up all individual predictions and save as a table
val regression_total=spark.sql("select zfg_temp.name, zfg_temp.year, zfg_temp.prediction + zft_temp.prediction as prediction, zfg_temp.label + zft_temp.label as label from zfg_temp, zft_temp where zfg_temp.name=zft_temp.name")
//regression_total.write.mode("overwrite").saveAsTable("basketball.regression_total")
regression_total.registerTempTable("predictions")

//show top 100 predicted players
//spark.sql(s"Select * from $dbName.regression_total order by prediction desc").show(100)
spark.sql(s"Select * from predictions order by prediction desc").show(100)
