package test.scala

import org.apache.spark.sql.functions._
import java.io._
import java.util.UUID

import breeze.linalg.{DenseMatrix, DenseVector}
import java.util.ArrayList

import IForest.{IForestOnSpark, IForestProperty, IForestSerializer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object IForestOnSparkTest {
  def main(args: Array[String]) {
    var filepath = "../Data/iforest/9701B9702B.csv"
    val spark = SparkSession.builder().master("local[4]")
      .appName("IFOREST").getOrCreate()

    var testfile = "../Data/iforest/test.csv"
    var test_set = spark.read.option("header", true).csv(testfile)

    var data_set = spark.read.option("header", true).csv(filepath)


    var prop = new IForestProperty
    prop.max_sample = 5000
    prop.n_estimators = 1500
    prop.max_depth_limit = (math.log(prop.max_sample) / math.log(2)).toInt
    prop.bootstrap = true
    prop.partition = 10

    var data_mtx = convertDFtoDM(data_set, data_set.count().toInt, 3)

    var ift = new IForestOnSpark(prop)
    ift.fit(data_mtx, spark)
    val writer = new PrintWriter("../Data/rs_IF_test.txt")
    var upbound = Array[Double](28.9, 31.7)
    var lowbound = Array[Double](18.5, 15.9)

    var step = Array[Double]((upbound(0).toDouble - lowbound(0).toDouble) / 500.0, (upbound(1).toDouble - lowbound(1).toDouble) / 500.0)
    var start = lowbound

    for (row <- 0 to 500 - 1) {
      println(row)
      for (col <- 0 to 500 - 1) {
        var x = DenseVector(start(0) + step(0) * row, start(1) + step(1) * col)
        var output = ift.predict(x)
        writer.print(1 - output + " ")
      }
      writer.print("\n")
    }
    writer.close()
    println("Predict finished")

    //Serialize model to HDFS
    var if_seralizer = new IForestSerializer
    if_seralizer.serialize("hdfs://127.0.0.1/ifserialized", ift)

    //Load model from HDFS
    var if_loader = new IForestSerializer
    var localmodel = if_loader.deserialize("hdfs://172.16.22.14:9000/ifserialized")
  }

  def convertDFtoDM(X: Dataset[Row], rows: Int, cols: Int): DenseMatrix[Double] = {
    var date_col = 1
    var mtx = new DenseMatrix[Double](rows, cols - date_col)
    var arr = X.rdd.map(row => {
      row.toSeq.toArray
    }).collect()

    for (row <- 0 until arr.size) {
      for (col <- 0 until cols - date_col) {
        mtx(row, col) = arr(row)(col + date_col).toString().toDouble
      }
    }
    return mtx
  }

}
