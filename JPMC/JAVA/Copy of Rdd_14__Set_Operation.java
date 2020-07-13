package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_14__Set_Operation {


public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> words1 = sc.parallelize(
				Arrays.asList("hello", "war", "peace", "world"));
		JavaRDD<String> words2 = sc.parallelize(
				Arrays.asList("war", "peace", "universe"));

		System.out.println("Example for Set operations : Union:");
		ExerciseUtils.printStringRDD(words1.union(words2), 10);
		
		
		
		System.out.println("Example for Set operations : Intersection");
		ExerciseUtils.printStringRDD(words1.intersection(words2), 10);
}

}
