package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class RDD_reduce {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Integer> data = Arrays.asList(1,2,3,4,5);


        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> collData = sc.parallelize(data);

        System.out.println("Data from RDD");

        collData.collect().forEach(System.out::println);

        int collCount = collData.reduce((x,y)->x+y);
        System.out.println("Count Data from RDD  "+ collCount);
        sc.close();
    }
}
