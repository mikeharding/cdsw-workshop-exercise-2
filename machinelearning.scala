val dbName = "basketball"

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ListBuffer

/* Create ML Table */

val sqlText=s"CREATE TABLE IF NOT EXISTS $dbName.ml as with t2 as (select * from $dbName.players UNION select distinct exp+1, name, 2018,age+1, Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null,Null from $dbName.players where year=2017 ) select t2.name, t2.year, t2.age, t2.exp, t2.team, t2.gp, t2.gs, t2.mp, coalesce (t1.ztot0,0) as ztot0, t2.ztot, coalesce (t1.ztotdiff,0) as ztotdiff, coalesce (t1.ntot0,0) as ntot0, t2.ntot, coalesce (t1.ntotdiff,0) as ntotdiff, coalesce (t1.zfg0,0) as zfg0, t2.zfg, coalesce (t1.zfgdiff,0) as zfgdiff, coalesce (t1.zft0,0) as zft0, t2.zft, coalesce (t1.zftdiff,0) as zftdiff, coalesce (t1.z3p0,0) as z3p0, t2.z3p, coalesce (t1.z3pdiff,0) as z3pdiff, coalesce (t1.ztrb0,0) as ztrb0, t2.ztrb, coalesce (t1.zdtrbiff,0) as ztrbdiff, coalesce (t1.zast0,0) as zast0, t2.zast, coalesce (t1.zastdiff,0) as zastdiff, coalesce (t1.zstl0,0) as zstl0, t2.zstl, coalesce (t1.zstldiff,0) as zstldiff, coalesce (t1.zblk0,0) as zblk0, t2.zblk, coalesce (t1.zblkdiff,0) as zblkdiff, coalesce (t1.ztov0,0) as ztov0, t2.ztov, coalesce (t1.ztovdiff,0) as ztovdiff, coalesce (t1.zpts0,0) as zpts0, t2.zpts, coalesce (t1.zptsdiff,0) as zptsdiff from t2 join ( select p2.name, p2.year, p2.age, p2.exp, p2.team, p2.gp, p2.gs, p2.mp, p1.ztot as ztot0, p2.ztot, (p2.zTot-p1.zTot) as ztotdiff, p1.ntot as ntot0, p2.ntot, (p2.nTot-p1.nTot) as ntotdiff, p1.zfg as zfg0, p2.zfg, (p2.zfg-p1.zfg) as zfgdiff, p1.zft as zft0, p2.zft, (p2.zft-p1.zft) as zftdiff, p1.z3p as z3p0, p2.z3p, (p2.z3p-p1.z3p) as z3pdiff, p1.ztrb as ztrb0, p2.ztrb, (p2.ztrb-p1.ztrb) as zdtrbiff, p1.zast as zast0, p2.zast, (p2.zast-p1.zast) as zastdiff, p1.zstl as zstl0, p2.zstl, (p2.zstl-p1.zstl) as zstldiff, p1.zblk as zblk0, p2.zblk, (p2.zblk-p1.zblk) as zblkdiff, p1.ztov as ztov0, p2.ztov, (p2.ztov-p1.ztov) as ztovdiff, p1.zpts as zpts0, p2.zpts, (p2.zpts-p1.zpts) as zptsdiff from t2 as p1 join t2 as p2 where p1.name=p2.name and p1.year=p2.year-1) as t1 on t2.name=t1.name and t2.year=t1.year"

spark.sql(sqlText)

//**********
//Computing Similar Players
//**********

//load in players data
val dfPlayers=spark.sql(s"select * from $dbName.players")
val pStats=dfPlayers.sort(dfPlayers("name"),dfPlayers("exp") asc).rdd.map(x=>(x.getString(1),(x.getDouble(50),x.getDouble(40),x.getInt(2),x.getInt(3),Array(x.getDouble(31),x.getDouble(32),x.getDouble(33),x.getDouble(34),x.getDouble(35),x.getDouble(36),x.getDouble(37),x.getDouble(38),x.getDouble(39)),x.getInt(0)))).groupByKey()
val excludeNames=dfPlayers.filter(dfPlayers("year")===1980).select(dfPlayers("name")).rdd.map(x=>x.mkString).collect().mkString(",")

//combine players seasons into one long array
val sStats = pStats.flatMap { case(player,stats) =>
     var exp:Int = 0
     var aggArr = Array[Double]()
     var eList = ListBuffer[(String, Int, Int, Array[Double])]()
     stats.foreach{ case(nTot,zTot,year,age,statline,experience) =>
          if (!excludeNames.contains(player)){
              aggArr ++= Array(statline(0), statline(1), statline(2), statline(3), statline(4), statline(5), statline(6), statline(7), statline(8))
              eList += ((player, exp, year, aggArr))
              exp+=1
          }}
          (eList)
}

//key by experience
val sStats1 = sStats.keyBy(x => x._2)

//match up players with everyone else of the same experience
val sStats2 = sStats1.join(sStats1)

//calculate distance
val sStats3 = sStats2.map { case(experience,(player1,player2)) => (experience,player1._1,player2._1,player1._3,math.sqrt(Vectors.sqdist(Vectors.dense(player1._4),Vectors.dense(player2._4)))/math.sqrt(Vectors.dense(player2._4).size))
}

//filter out players compared to themselves and convert to Row object
val similarity = sStats3.filter(x => (x._2!=x._3)).map(x => Row(x._1,x._4,x._2,x._3,x._5))

//schema for similar players
val schemaS = StructType(
     StructField("experience", IntegerType, true) ::
     StructField("year", IntegerType, true) ::
     StructField("name", StringType, true) ::
     StructField("similar_player", StringType, true) ::
     StructField("similarity_score", DoubleType, true) :: Nil
)


//create data frame
val dfSimilar = spark.createDataFrame(similarity,schemaS)
dfSimilar.cache

//save as table
dfSimilar.write.mode("overwrite").saveAsTable(s"$dbName.similar")


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
  val trainData = spark.sql(s"select name, year, exp, mp, $stat0, $stat as label from $dbName.ml where year<2018")
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
regression_total.write.mode("overwrite").saveAsTable("basketball.regression_total")


//show top 100 predicted players
spark.sql(s"Select * from $dbName.regression_total order by prediction desc").show(100)
