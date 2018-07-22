import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object FirstApp{

  def main(args: Array[String]): Unit = {
    println("Hello Wrold")
    val conf = new SparkConf().setAppName("First App").setMaster("local[4]")
    val sc=new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    println(distData.count())
  }

}
