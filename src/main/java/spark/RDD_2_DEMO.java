package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;


public class RDD_2_DEMO {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> autoAllData = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\test.csv");
        System.out.println("Count is " + autoAllData.count());

        sc.close();

    }
}
