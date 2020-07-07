package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class RDD_set_demo {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<String> word1 = Arrays.asList("hello","war","peace","world");
        List<String> word2 = Arrays.asList("war","peace","universe");

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> word1rdd = sc.parallelize(word1);
        JavaRDD<String> word2rdd = sc.parallelize(word2);


        word1rdd.union(word2rdd).collect().forEach(System.out::println);
        System.out.println("###########  Distinct ##########");
        word1rdd.union(word2rdd).distinct().collect().forEach(System.out::println);

        sc.close();
    }
}
