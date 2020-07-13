package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class Rdd_12__FlatMap {
public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
				
		// Create a RDD from a file
				JavaRDD<String> autoAllData = sc.textFile("target/My_Data/auto-data.csv");
				System.out.println("Total Records in Autodata : "   + autoAllData.count());
				System.out.println("Spark Operations : Load from CSV");
						
	// Filter Example : Filter data for only "toyota" with inline lambda
				// function
		JavaRDD<String> toyotaData = autoAllData.filter(str -> str.contains("toyota"));
				System.out.println("Spark Operations : FILTER");
				ExerciseUtils.printStringRDD(toyotaData, 5);
				// FlatMap with inline function
				System.out.println("Spark Operations : FLAT MAP");

				JavaRDD<String> words 
		            = toyotaData.flatMap(new FlatMapFunction<String, String>() {

					public Iterator<String> call(String s) {
						return Arrays.asList(s.split(",")).iterator();
					}
				});

				System.out.println("Toyota words count :" + words.count());
				ExerciseUtils.printStringRDD(words, 10);
				
}


}
