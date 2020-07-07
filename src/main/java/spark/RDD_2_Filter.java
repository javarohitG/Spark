package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.janino.Java;

import java.util.Arrays;
import java.util.Iterator;


public class RDD_2_Filter {
    public static void main(String a[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> autoAllData = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\auto-data.csv",2);
        System.out.println("Count is " + autoAllData.count());
        JavaRDD<String> mapWord = autoAllData.map((x)->(x));

        JavaRDD<String> toyotaTSV = autoAllData.map(str->str.replace(",","\t"));

        String header = toyotaTSV.first();

        JavaRDD<String>  noHeaderData = toyotaTSV.filter(s->!s.equals(header));

        JavaRDD<String>  toyotaFilterData = noHeaderData.filter(stri->stri.contains("toyota"));

        JavaRDD<String> toyotaDistinct = toyotaFilterData.distinct();

        toyotaFilterData.collect().forEach(System.out::println);

        System.out.println("Count without distinct "+ toyotaFilterData.count());
        System.out.println("Count without distinct "+ toyotaDistinct.count());
        sc.close();
    }
}
