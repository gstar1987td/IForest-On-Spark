package IForest

import java.net.URI
import java.util.UUID
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}

class IForestSerializer extends Serializable {
  def serialize(dst: String,IF:IForestOnSpark) {
    var osName = System.getProperty("os.name").toLowerCase()
    var name = this.toString().replace("@", "").replace(".", "") + UUID.randomUUID()
    var localFilePath = ""
    if (osName.indexOf("linux") >= 0) {
      localFilePath = "/tmp/" + name + ".txt"
    } else if (osName.indexOf("windows") >= 0) {
      localFilePath = "c:\\windows\\temp\\" + name + ".txt"
    } else {
      localFilePath = name + ".txt"
    }
    serialize_file(localFilePath,IF)
    send(localFilePath, dst)
  }

  //序列化到本地文件，其中file是本地文件的完整路径
  private def serialize_file(localFilePath: String,IF:IForestOnSpark) = {
    val file = new File(localFilePath)
    file.setWritable(true)
    file.setReadable(true)
    file.createNewFile()

    val bos = new FileOutputStream(localFilePath)

    val out = new LZ4BlockOutputStream(bos)
    out.flush()

    val oos = new ObjectOutputStream(out)
    oos.writeObject(IF)

    oos.flush()
    oos.close()
    bos.flush()
    bos.close()
  }

  private def send(src: String, dest: String) {
    val conf = new Configuration()
    conf.set("hadoop.job.ugi", "spark")

    val fs = FileSystem.newInstance(URI.create(dest), conf)
    //目标完整路径
    val dstPath = new Path(dest)
    //本地文件完整路径
    val srcPath = new Path(src)
    //从本地copy文件到HDFS，第一个参数是bool类型：true 删除本地文件，false 保留本地文件（默认为false）
    fs.copyFromLocalFile(true, srcPath, dstPath);
    fs.setPermission(dstPath, FsPermission.valueOf("-rwxrw-rw-"))
    fs.close()
  }
  //  反序列化到本地文件，返回一个nnetworkd对象；dest ：HDFS文件完整地址；  local：本地文件完整地址
  def deserialize(dest: String): IForestOnSpark = {
    var osName = System.getProperty("os.name").toLowerCase()
    var localFilePath = ""
    if (osName.indexOf("linux") >= 0) {
      localFilePath = "/tmp/" + this.toString().replace("@", "").replace(".", "")
    } else if (osName.indexOf("windows") >= 0) {
      localFilePath = "c:\\windows\\temp\\" + this.toString().replace("@", "").replace(".", "")
    } else {
      localFilePath = this.toString().replace("@", "").replace(".", "")
    }
    load(localFilePath, dest)
    var IF = deserialize_file(localFilePath)
    new File(localFilePath).delete()
    IF
  }

  //从HDFS将文件下载到本地 srcFilePath:HDFS文件完整地址
  private def load(localFilePath: String, srcFilePath: String) {
    val file = new File(localFilePath)
    file.createNewFile()
    val conf = new Configuration();
    conf.set("hadoop.job.ugi", "spark,spark")

    val fs = FileSystem.newInstance(URI.create(srcFilePath), conf)
    val fsdi = fs.open(new Path(srcFilePath));
    val output = new FileOutputStream(localFilePath);
    //从fsdi流拷贝到outPut流 ， 4096 代表缓冲区大小；true - 是否关闭数据流，如果是false，就在finally里关掉
    IOUtils.copyBytes(fsdi, output, 4096, true);
    output.flush()
    fs.close()
  }

  //根据file（本地文件的完整路径）上反序列化一个nnetwork对象，该对象是本对象的一个实例化
  def deserialize_file(localFilePath: String): IForestOnSpark = {
    val bis = new FileInputStream(localFilePath)

    val lz4 = new LZ4BlockInputStream(bis)

    val ois = new ObjectInputStream(lz4)
    //---------------------------------------deserialize object
    //    var trainingData: Dataset[Row] = _
    //    var testDf: Dataset[Row] = _
    //    var m_inputsIDs = ArrayBuffer[String]()
    //    var m_outputsIDs = ArrayBuffer[String]()
    //
    //    var vectorAssembler_input: VectorAssembler = _
    //    var vectorAssembler_output: VectorAssembler = _
    //    var model: PipelineModel = _
    //    var prediction: DataFrame = _

    var IFinstance = ois.readObject.asInstanceOf[IForestOnSpark]

    ois.close()
    bis.close()
    return IFinstance
  }
}