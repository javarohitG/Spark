package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Rdd_Assignment3_acc {

	public static void main(String args[]) throws InterruptedException {


		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("Day3_demo").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> myCSVRdd = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\auto-data.csv");

		String header = myCSVRdd.first();

		JavaRDD<String> myCSVRdd_data = myCSVRdd.filter(s -> !s.equals(header));


		LongAccumulator sedanCount = sc.sc().longAccumulator();
		LongAccumulator hatchbackCount = sc.sc().longAccumulator();

		Broadcast<String> sedanText = sc.broadcast("sedan");
		Broadcast<String> hatchbackText = sc.broadcast("hatchback");

		JavaRDD<String> autoOut = myCSVRdd_data.map(new Function<String, String>() {
			public String call(String x) {

				if (x.contains(sedanText.value())) {
					sedanCount.add(1);
				}
				if (x.contains(hatchbackText.value())) {
					hatchbackCount.add(1);
				}
				return x;
			}
		});

		autoOut.count();

		System.out.println("Demo for Accumulators and Broadcasts : ");
		System.out.println("Sedan Count : " + sedanCount.value() +
				"  HatchBack Count : " + hatchbackCount.value());
		sc.close();
	}
}