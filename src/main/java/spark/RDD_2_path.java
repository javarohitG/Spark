package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


public class RDD_2_path {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> autoAllData = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\auto-data.csv",2);
        System.out.println("Count is " + autoAllData.count());

        String shortest = autoAllData.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return (s.length()<s2.length() ? s:s2);
            }
        });

        System.out.println("The Shorest String ::  "+shortest);
        sc.close();
    }
}

