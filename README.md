This project Implement the article of :

Liu, Fei Tony, Kai Ming Ting, and Zhi-Hua Zhou. "Isolation forest."Data Mining, 2008. ICDM'08. Eighth IEEE International Conference on. IEEE, 2008.



IForest On Spark use spark to sampling data, and separate each partitoin to a spark worker.
Each partition train n isolate trees. The train process is runing on paralle mode.    

The prediction uses all isolation trees trained by spark, to predict the outlier factors.



SKLearn Iforest:
http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html

Comparation:
SKLearn Iforest:
http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html

SVM OneClass:
SVM OneClass Result:
![image](https://github.com/gstar1987td/IForest-On-Spark/blob/master/result/Figure_1-1_svm.png)

IForest On Spark:

IForest On Spark Test Result:
![image](https://github.com/gstar1987td/IForest-On-Spark/blob/master/result/10Partition1500Tree500Samples.png)


How To Use:
```Scala 
   var prop = new IForestProperty
    prop.max_sample = 5000
    prop.n_estimators = 1500
    prop.max_depth_limit = (math.log(prop.max_sample) / math.log(2)).toInt
    prop.bootstrap = true
    prop.partition = 10
    
    var ift = new IForestOnSpark(prop)
    var data_mtx:DenseMatrix[Double] = ... (train data in matrix)
    ift.fit(data_mtx, spark)
    
    x: DenseVector[Double] = ... (test data)
    var output = ift.predict(x)
    
    //Serialize model to HDFS
    var if_seralizer = new IForestSerializer
    if_seralizer.serialize("hdfs://127.0.0.1/ifserialized", ift)

    //Load model from HDFS
    var if_loader = new IForestSerializer
    var localmodel = if_loader.deserialize("hdfs://172.16.22.14:9000/ifserialized")
 ```
    
    
