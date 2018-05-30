package IForest

import breeze.linalg.{DenseVector,DenseMatrix}
import scala.util.control.Breaks._
import org.apache.spark.sql.{Dataset,SparkSession,Row,functions}

class Node extends Serializable
{
  var left_node:Node = _
  var right_node:Node = _
  var p = 0.0
  var depth = 0
  var q = 0
  var is_externel = false
  var max = 0D
  var min = 0D
}

class ITree (max_depth_limit:Int) extends Serializable{
  //var tree:ArrayList[Node] = new ArrayList()
  var head = new Node
  var max_depth = 0
  var n = 0
  private var C = 0.57721566490153286060651209

  //Dataset
  var n_row = 0L
  private var date_col = 0
  private var columns :Array[String] = null

  def setC(newC:Double){
    C = newC
  }

  def train(Data:DenseMatrix[Double])
  {
    if(Data!=null && Data.rows>2)
    {
      n=Data.cols
      var node = new Node
      var q_rnd = (new scala.util.Random).nextInt(Data.cols)
      node.max = Data(::,q_rnd).max
      node.min = Data(::,q_rnd).min
      if(node.max==node.min)
      {
        return
      }

      //var p_rnd = (new util.Random).nextDouble()
      //p_rnd = p_rnd*(node.max-node.min) + node.min
      var p_rnd_index = (new scala.util.Random).nextInt(Data.rows)
      var p_rnd = Data(p_rnd_index,q_rnd)

      node.p = p_rnd
      node.q = q_rnd
      var left = new DenseMatrix[Double](0,Data.cols)
      var right = new DenseMatrix[Double](0,Data.cols)
      for(i<- 0 to Data.rows-1)
      {
        //if(i!=p_rnd)
        //{
        if(Data(i,q_rnd)<node.p)
        {
          var tmp = new DenseMatrix[Double](1,Data.cols)
          for(n<- 0 to Data.cols-1)
          {
            tmp(0,n) = Data(i,n)
          }
          left = DenseMatrix.vertcat(left,tmp)
        }
        else if(Data(i,q_rnd)>=node.p)
        {
          var tmp = new DenseMatrix[Double](1,Data.cols)
          for(n<- 0 to Data.cols-1)
          {
            tmp(0,n) = Data(i,n)
          }
          right = DenseMatrix.vertcat(right,tmp)
        }
        //}
      }

      node.left_node = get_node(left,0)
      node.right_node = get_node(right,0)

      node.depth = 0
      head = node
      //tree.add(node)
    }
  }

  def get_node(Data:DenseMatrix[Double],current_depth:Int):Node=
  {
    if(current_depth>max_depth)
    {
      max_depth = current_depth
    }
    if(current_depth==max_depth_limit)
    {
      return null
    }
    if(Data!=null && Data.rows>1)
    {
      var q_rnd = (new scala.util.Random).nextInt(Data.cols)
      var node = new Node
      node.max = Data(::,q_rnd).max
      node.min = Data(::,q_rnd).min
      if(node.max==node.min)
      {
        return null
      }
      node.depth = current_depth+1

      var p_rnd = (new scala.util.Random).nextDouble()
      p_rnd = p_rnd*(node.max-node.min) + node.min

      node.p = p_rnd
      node.q = q_rnd
      var left = new DenseMatrix[Double](0,Data.cols)
      var right = new DenseMatrix[Double](0,Data.cols)

      for(i<- 0 to Data.rows-1)
      {
        //if(i!=p_rnd)
        //{
        if(Data(i,q_rnd)<node.p)
        {
          var tmp = new DenseMatrix[Double](1,Data.cols)
          for(n<- 0 to Data.cols-1)
          {
            tmp(0,n) = Data(i,n)
          }
          left = DenseMatrix.vertcat(left,tmp)
        }
        else if(Data(i,q_rnd)>=node.p)
        {
          var tmp = new DenseMatrix[Double](1,Data.cols)
          for(n<- 0 to Data.cols-1)
          {
            tmp(0,n) = Data(i,n)
          }
          right = DenseMatrix.vertcat(right,tmp)
        }
        //}
      }

      node.left_node = get_node(left,node.depth)
      node.right_node = get_node(right,node.depth)

      return node
    }
    else if(Data.rows==1)
    {
      var node = new Node
      node.p = Data(0,0)
      return node
    }
    else
    {
      return null
    }
  }


  def get_data_dept(x:DenseVector[Double]):Double=
  {
    var node_ptr = head
    breakable
    {
      while(node_ptr.left_node!=null && node_ptr.right_node!=null)
      {
        if(x(node_ptr.q)<node_ptr.p)
        {
          node_ptr = node_ptr.left_node
        }
        else if(x(node_ptr.q)>=node_ptr.p)
        {
          node_ptr = node_ptr.right_node
        }
      }
    }
    return node_ptr.depth
    //return node_ptr.depth + (2*H(n-1) - (2*(n-1)/n))
  }

  def get_score(x:DenseVector[Double]):Double=
  {
    var node_ptr = head
    breakable
    {
      while(node_ptr.left_node!=null && node_ptr.right_node!=null)
      {
        if(x(node_ptr.q)<node_ptr.p)
        {
          node_ptr = node_ptr.left_node
        }
        else if(x(node_ptr.q)>=node_ptr.p)
        {
          node_ptr = node_ptr.right_node
        }
      }
    }
    return node_ptr.depth + (2*H(n-1) - (2*(n-1)/n))
  }

  def get_score(x:Row):Double=
  {
    var node_ptr = head
    breakable
    {
      while(node_ptr.left_node!=null && node_ptr.right_node!=null)
      {
        if(x.getDouble(node_ptr.q)<node_ptr.p)
        {
          node_ptr = node_ptr.left_node
        }
        else if(x.getDouble(node_ptr.q)>=node_ptr.p)
        {
          node_ptr = node_ptr.right_node
        }
      }
    }
    return node_ptr.depth + (2*H(n-1) - (2*(n-1)/n))
  }



  def H(i:Double):Double=
  {
    return math.log(i) + C
  }

}