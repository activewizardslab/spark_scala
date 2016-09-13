import scala.collection.mutable.MutableList
import java.io._
import math._
  
var r = 20 // define the range size 
val channels = List("Fc5", "Fc3", "Fc6")
val path1 = "/home/terrapin/Downloads/eegmmidb_S001_S001R011.csv"
val path2 = "/home/terrapin/Downloads/eegmmidb_S002_S002R011.csv"
  
  
object LB_KEOGH{
  // read the files with Time series and create an rdd for each file
  def read_file(path: String, channels: List[String]) : org.apache.spark.rdd.RDD[List[String]] = {
    val rdd1 = sc.textFile(path, 2).map { line =>  line.split(',')} // read text file by line and split line by comma
    val rdd11 = rdd1.zipWithIndex().filter(_._2 > 1) // drop the first row with names of columns
    val f = rdd1.first() // save names of columns into variable f
    val f2 = f.map{x => x.split('.')(0) slice (1, x.length -1)} // get the names of channels from the first column	
    val channels1 = channels.map{x => f2.indexOf(x)} // get the indexes of channels
    val data11 = rdd11.map(x => x._1).map(x => channels1.map(c => x(c))) // map rdd with time series by names of channels
    return data11
  }
  
  // calculate Keogh lower bound
  def LB_Keogh(t1: org.apache.spark.rdd.RDD[(Float, Long)], t2: org.apache.spark.rdd.RDD[(Float, Long)], r: Int) : Double = {
    val t1_n = t1.count().toInt // save the number of time points in the first rdd
    var LB_sum = 0.0 // default LB_sum value
  
    for (ind <- 0 until t1_n) {
      var available_indices = ((if (ind-r >= 0) ind-r else 0) to (ind+r)).toArray; // generate an array with indices in range [i-r, i+r]
      var vals = t2.filter(x => available_indices contains x._2).map(x => x._1); // values for min and max selection
      var current_value = t1.filter(x => x._2 == ind).map(x => x._1).take(1)(0); // `ind`th value fo `t1` rdd
      var lower_bound = vals.min(); // find min
      var upper_bound = vals.max(); // find max
      if (current_value > upper_bound) { // increase the `LB_sum` value depending of `current_value` 
        LB_sum += pow((current_value-upper_bound), 2)
      } else if (current_value < lower_bound) {
        LB_sum += pow((current_value-lower_bound), 2)
      }
    }
    return sqrt(LB_sum)
  }  
  
  // print the data to file
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  
  
  // calculate the LB_Keogh for all channels and save the data into file
  def result_writting(path1: String, path2: String, channels: List[String]): scala.collection.mutable.MutableList[(String, Double)] = {
    val data11 = read_file(path1, channels)
    val data22 = read_file(path2, channels)
    val p = 0;
    val m_res = MutableList(("channel", 0.0));
    for (p <- 0 until channels.length){
      var t1 = data11.map(x => x(p).toFloat).zipWithIndex();
      var t2 = data22.map(x => x(p).toFloat).zipWithIndex();
      var res = LB_Keogh(t1,t2,r);
      println("Channel = " + channels(p));
      println("LB_Keogh = " + res);
      m_res += ((channels(p), res));
    }
    printToFile(new File("result.txt")) { p =>m_res.foreach(p.println) }
    return m_res}
  }