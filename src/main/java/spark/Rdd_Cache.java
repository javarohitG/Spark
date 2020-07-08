package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class Rdd_Cache {

	public static void main(String args[]) throws InterruptedException {


		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("Demo").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> myCSVRdd = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\auto-data.csv",8);

		String header = myCSVRdd.first();

		JavaRDD<String> myCSVRdd_data = myCSVRdd.filter(s -> !s.equals(header));

		myCSVRdd_data.cache();

		System.out.println("No. of partitions in autoData = " + myCSVRdd_data.getNumPartitions());

		JavaRDD<String> wordsList = sc.parallelize(Arrays.asList("hello", "war", "peace", "world"), 4);

		System.out.println("No. of partitions in wordsList = " +
				wordsList.getNumPartitions());

//		System.out.println("Demo for Accumulators and Broadcasts : ");
//		System.out.println("Sedan Count : " + sedanCount.value() +
//				"  HatchBack Count : " + hatchbackCount.value());
		sc.close();
	}
}