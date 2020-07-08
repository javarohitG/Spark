//package spark;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//
//import java.util.Arrays;
//import java.util.List;
//
//
//public class RDD_wordcout {
//    public static void main(String a[]){
//
//        Logger.getLogger("org.apache").setLevel(Level.WARN);
//
//
//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaRDD<String> autoAllData = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\abc.txt",2);
//
//        //JavaRDD<Integer> count = autoAllData.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_) counts.collect();
//
//
//
//        sc.close();
//    }
//}
