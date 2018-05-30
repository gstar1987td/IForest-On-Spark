package IForest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession, Row }
import breeze.linalg.{ DenseVector, DenseMatrix }
import scala.collection.mutable.ArrayBuffer

class IForestProperty extends Serializable {
  var n_estimators = 100
  var max_sample = 0L
  var contamination = 0.1
  var max_feature = 0
  var max_depth_limit = 20
  var bootstrap = false
  var C = 0.57721566490153286060651209
  var DateCol = true
  var partition = 5
}

class ITreeGroup extends Serializable {
  var GroupSize = 0
  var dataset: DenseMatrix[Double] = null
}

class IForestOnSpark(prop: IForestProperty) extends Serializable {
  private var params = prop
  private var ITrees: Array[ITree] = null
  private var spark_session: SparkSession = _
  //private var date_col = 0

  def fit(X: DenseMatrix[Double], spark_session: SparkSession) {
    var data_size = X.rows
    var feature_size = X.cols
    var data_set = convertDMtoRDD(X, spark_session)
    if (params.max_sample == 0) {
      params.max_sample = data_size
      println("Param's max_sample is 0,use whole dataset:" + data_size)
    }
    if (params.contamination > 1 || params.contamination <= 0) {
      println("Param's contamination out of range:" + params.contamination)
      return
    }
    if (params.max_feature == 0) {
      params.max_feature = feature_size
      println("Param's max_feature is 0, use all features:" + params.max_feature)
    }
    if (params.max_depth_limit == 0) {
      params.max_depth_limit = (math.log(data_size.toDouble) / math.log(2)).toInt
      println("Param's max_depth_limit is 0, use default depth:" + params.max_depth_limit)
    }
    println("*************************************")

    println("Max Tree Depth:" + params.max_depth_limit)
    println("Sample Size:" + params.max_sample)
    var fraction = params.max_sample.toDouble / data_size.toDouble
    println("Fraction:" + fraction)
    println("Spark Partition:" + params.partition)
    println("Sample Bootstrap:" + params.bootstrap)

    var group = new Array[ITreeGroup](params.partition)
    for (i <- 0 until group.size) {
      println(i)
      group(i) = new ITreeGroup()
      group(i).GroupSize = params.n_estimators / params.partition
      var sample_set = data_set.sample(params.bootstrap, fraction)
      group(i).dataset = convertRDDtoDM(sample_set, sample_set.count().toInt, feature_size)
    }

    println("Data Sampling Finished.")
    println("*************************************")
    println("Begin Training:")

    var group_rdd = spark_session.sparkContext.parallelize(group)
    group_rdd.repartition(params.partition)

    var forest = group_rdd.mapPartitions[ITree](groups => {
      var res = ArrayBuffer[ITree]()
      while (groups.hasNext) {
        var group = groups.next()
        for (i <- 0 until group.GroupSize) {
          var itree = new ITree(params.max_depth_limit)
          itree.train(group.dataset)
          res.append(itree)
          println("Append" + i)
        }
      }
      res.iterator
    })
    ITrees = forest.collect()
    println("Training Finished. IForest Size:" + ITrees.size)
  }

  def fit(X: Dataset[Row]) {
    spark_session = X.sparkSession
    X.show(10)
    var data_size = X.count()
    var feature_size = X.columns.size
    if (params.max_sample == 0) {
      params.max_sample = data_size
      println("Param's max_sample is 0,use whole dataset:" + data_size)
    }
    if (params.contamination > 1 || params.contamination <= 0) {
      println("Param's contamination out of range:" + params.contamination)
      return
    }
    if (params.max_feature == 0) {
      params.max_feature = feature_size
      println("Param's max_feature is 0, use all features:" + params.max_feature)
    }
    if (params.max_depth_limit == 0) {
      params.max_depth_limit = (math.log(data_size.toDouble) / math.log(2)).toInt
      println("Param's max_depth_limit is 0, use default depth:" + params.max_depth_limit)
    }
    println("*************************************")

    println("Max Tree Depth:" + params.max_depth_limit)
    println("Sample Size:" + params.max_sample)
    var fraction = params.max_sample.toDouble / data_size.toDouble
    println("Fraction:" + fraction)
    println("Spark Partition:" + params.partition)
    println("Sample Bootstrap:" + params.bootstrap)

    var group = new Array[ITreeGroup](params.partition)
    for (i <- 0 until group.size) {
      println(i)
      group(i) = new ITreeGroup()
      group(i).GroupSize = params.n_estimators / params.partition
      var sample_set = X.sample(params.bootstrap, fraction)
      group(i).dataset = convertDFtoDM(sample_set, sample_set.count().toInt, feature_size)
    }

    println("Data Sampling Finished.")
    println("*************************************")
    println("Begin Training:")

    var group_rdd = spark_session.sparkContext.parallelize(group)
    group_rdd.repartition(params.partition)

    var forest = group_rdd.mapPartitions[ITree](groups => {
      var res = ArrayBuffer[ITree]()
      while (groups.hasNext) {
        var group = groups.next()
        for (i <- 0 until group.GroupSize) {
          var itree = new ITree(params.max_depth_limit)
          itree.train(group.dataset)
          res.append(itree)
          println("Append" + i)
        }
      }
      res.iterator
    })
    ITrees = forest.collect()

    println("Training Finished. IForest Size:" + ITrees.size)
  }

  def predict(x: Row): Double =
  {
    var sum = 0D
    for (i <- 0 until ITrees.size) {
      sum += ITrees(i).get_score(x)
    }
    var E_h_x = sum.toDouble / ITrees.size.toDouble
    var n = params.max_sample
    var c_n = 2 * H(n - 1) - (2 * (n - 1) / n)
    return math.pow(2, -(E_h_x / c_n))
  }

  def predict(x: DenseVector[Double]): Double =
  {
    var sum = 0D
    for (i <- 0 until ITrees.size) {
      sum += ITrees(i).get_score(x)
    }
    var E_h_x = sum.toDouble / ITrees.size.toDouble
    var n = params.max_sample
    var c_n = 2 * H(n - 1) - (2 * (n - 1) / n)
    return math.pow(2, -(E_h_x / c_n))
  }

  private def H(i: Double): Double =
  {
    return math.log(i) + params.C
  }

  private def convertDFtoDM(X: Dataset[Row], rows: Int, cols: Int): DenseMatrix[Double] = {
    var date_col = 0
    if (params.DateCol == true) {
      date_col = 1
    }
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

  private def convertDMtoRDD(matrix: DenseMatrix[Double], spark: SparkSession): RDD[Array[Double]] =
  {
    var rows = matrix.rows
    var cols = matrix.cols

    var data_rdd: RDD[Array[Double]] = null
    var temp_arr = new Array[Array[Double]](rows)

    for (i <- 0 until rows) {
      var row_arr = matrix(i, ::).inner.toArray
      //var row_arr = matrix(i,::).asInstanceOf[DenseVector[Double]].toArray
      temp_arr(i) = row_arr
    }
    data_rdd = spark.sparkContext.makeRDD(temp_arr)
    return data_rdd
  }

  private def convertRDDtoDM(rdd: RDD[Array[Double]], rows: Int, cols: Int): DenseMatrix[Double] =
  {
    var data_mtx = new DenseMatrix[Double](rows, cols)
    var arr = rdd.collect()
    for (n <- 0 until arr.length) {
      var row_data = arr(n)
      for (m <- 0 until row_data.length) {
        data_mtx(n, m) = row_data(m)
      }
    }
    return data_mtx
  }
}
