package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Rdd_12__FlatMAP_Nes {
	 public static void main(String a[]){

	        Logger.getLogger("org.apache").setLevel(Level.WARN);
	        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);

	        JavaRDD<String> autoAllData = sc.textFile("target/My_Data/abc.txt",2);
	        System.out.println("Count is " + autoAllData.count());

	        JavaRDD<String> word = autoAllData.flatMap(new FlatMapFunction<String, String>() {
	            @Override
	            public Iterator<String> call(String s) throws Exception {
	                return Arrays.asList(s.split(" ")).iterator();
	            }
	        });
	        System.out.println(""+word.count());
	        sc.close();

	    }

}
