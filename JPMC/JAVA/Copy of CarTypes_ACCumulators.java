package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
public class CarTypes_ACCumulators {
    public static void main(String args[]) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a RDD from a file
        JavaRDD<String> autoAllData = sc.textFile("target/auto-data.csv");
        System.out.println("Total Records in Autodata :" + autoAllData.count());
        System.out.println("Spark Operations : Load from CSV");

        //Remove first header line
        String header = autoAllData.first();
        JavaRDD<String> autoData = autoAllData.filter(s -> !s.equals(header));
        	/*-------------------------------------------------------------------
		 * Accumulators and Broadcast variables.
		 -------------------------------------------------------------------*/

        LongAccumulator sedanCount = sc.sc().longAccumulator();
        LongAccumulator hatchbackCount = sc.sc().longAccumulator();

        Broadcast<String> sedanText = sc.broadcast("sedan");
        Broadcast<String> hatchbackText = sc.broadcast("hatchback");

        JavaRDD<String> autoOut
                = autoData.map(new Function<String, String>() {
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

        // Execute an action to force the map. Otherwise accumulators are not
        // triggered.
        autoOut.count();

        System.out.println("Demo for Accumulators and Broadcasts : ");
        System.out.println("Sedan Count : " + sedanCount.value() +
                "  HatchBack Count : " + hatchbackCount.value());


    }
}