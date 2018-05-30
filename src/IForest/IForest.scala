package IForest

import java.util.ArrayList
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import org.apache.spark.sql.SparkSession

class IForest(spark:SparkSession,ITree_num:Int,Sample_count:Int,max_depth_limit:Int) extends Serializable {
  var C = 0.57721566490153286060651209
  var n = 0

  var TreeList = new ArrayList[ITree]()

  def fit(Data:DenseMatrix[Double])
  {
    //n = Data.length
    //n = ITree_num
    //n = Data.cols
    n = 8000
    for(i<- 1 to ITree_num)
    {
      var itree = new ITree(max_depth_limit)
      itree.train(Data)
      TreeList.add(itree)
    }
  }

  def get_Anomaly_Score(x:DenseVector[Double]):Double=
  {
    var sum_depth = 0.0
    for(i<- 0 until TreeList.size())
    {
      sum_depth+=TreeList.get(i).get_data_dept(x)
    }
    var E_h_x = sum_depth.toDouble/TreeList.size().toDouble
    //print(" E:"+E_h_x)
    var c_n = 2*H(n-1) - (2*(n-1)/n)
    //print(math.pow(2,3))

    //return E_h_x
    return math.pow(2,-(E_h_x/c_n))
  }

  def H(i:Double):Double=
  {
    return math.log(i) + C
  }
}
