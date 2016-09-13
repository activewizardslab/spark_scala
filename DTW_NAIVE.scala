

import scala.collection.mutable.MutableList
import java.io._

val channels = List("Fc5", "Fc3", "Fc6")
val path1 = "/home/terrapin/Downloads/eegmmidb_S001_S001R011.csv"
val path2 = "/home/terrapin/Downloads/eegmmidb_S002_S002R011.csv"


object DTW_NAIVE{
	// read the files with Time series and create an rdd for each file
	def read_file(path: String, channels: List[String]) : org.apache.spark.rdd.RDD[List[String]] = {
		val rdd1 = sc.textFile(path, 2).map { line =>  line.split(',')} // read text file by line and split line by comma
		val rdd11 = rdd1.zipWithIndex().filter(_._2 > 1) // drop the first row with names of columns
		val f = rdd1.first() // save names of columns into variable f
		val f2 = f.map{x => x.split('.')(0) slice (1, x.length -1)} // get the names of channels from the first column	
		val channels1 = channels.map{x => f2.indexOf(x)} // get the indexes of channels
		val data11 = rdd11.map(x => x._1).map(x => channels1.map(c => x(c))) // map rdd with time series by names of channels
		return data11}
	
	// calculate DTW
	def DTW_naive(t1: org.apache.spark.rdd.RDD[(Float, Long)], t2: org.apache.spark.rdd.RDD[(Float, Long)]) : Double = {	
		val t = t1.cartesian(t2) // get an rdd with cartesian product from 2 rdds with time series
		val dist = t.map(x => ((x._1._2.toInt, x._2._2.toInt), ((x._1._1 - x._2._1)*(x._1._1 - x._2._1)).toDouble)) // calculate the distance between time points
		val t1_n = t1.count().toInt // save the number of time points in the first rdd
		val t2_n = t2.count().toInt // save the number of time points in the second rdd
		val zero = 0.0
		val DTW = MutableList(((-1,-1), zero.toDouble )) // create the Mutable list with matrix points (-1,-1)
		var y = 0;
		for ( y <- 0 to t1_n-1){
	 	  DTW += (((y,-1), 2.0/0)); // define infinity in -1 points
		}
		var z = 0
		for ( z <- 0 to t2_n-1){
		   DTW += (((-1,z), 2.0/0)); // define infinity in -1 points
		}
		val dtw_rdd = sc.parallelize(DTW) // move Mutable List to rdd
		var dtw_path = dtw_rdd.union(dist).cache() // union list with all time points
		val i=0;
		val j=0;
		var minimum = 0.0
		for (i <- 0 until t1_n; j <- 0 until t2_n){ // iterate through matrix	
		var temp = Array((i-1,j), (i,j-1), (i-1,j-1)); // define nearest points of the matrix
		minimum= dtw_path.filter(x=> temp contains x._1).map(x =>x._2).min(); // search by minimum value throught nearest points
		dtw_path = dtw_path.map(x => if (x._1 == (i,j)) (x._1, x._2+minimum) else x).cache(); // add the minimum value
		}
		val dtw1 = dtw_path.filter(x => x._1 == (t1_n-1,t2_n-1)).map(x => x._2).collect() // select the last time point (n,m)
		val dtw = math.sqrt(dtw1(0)) // calculate the root from the value in last point
		return dtw }

	// print the data to file
	def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
	  val p = new java.io.PrintWriter(f)
	  try { op(p) } finally { p.close() }
	}


	// calculate the DTW for all channels and save the data into file
	def result_writting(path1: String, path2: String, channels: List[String]): scala.collection.mutable.MutableList[(String, Double)] = {
		val data11 = read_file(path1, channels)
		val data22 = read_file(path2, channels)
		val p = 0;
		val m_res = MutableList(("channel", 0.0));
		for (p <- 0 until channels.length){
			var t1 = data11.map(x => x(p).toFloat).zipWithIndex();
			var t2 = data22.map(x => x(p).toFloat).zipWithIndex();
			var res = DTW_naive(t1,t2);
			println("Channel = " + channels(p));
			println("DTW = " + res);		
			m_res += ((channels(p), res));}
	
		printToFile(new File("result.txt")) { p =>
		  m_res.foreach(p.println)
		}
		return m_res}
}













				

