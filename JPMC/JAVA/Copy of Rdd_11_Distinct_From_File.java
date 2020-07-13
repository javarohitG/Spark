package com.manishSparkJavaspark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_11_Distinct_From_File {

public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
				
		// Create a RDD from a file
				JavaRDD<String> autoAllData = sc.textFile("target/My_Data/auto-data.csv");
				System.out.println("Total Records in Autodata : "   + autoAllData.count());
				System.out.println("Spark Operations : Load from CSV");
						

		// Distinct Example
		//System.out.println("Spark Distinct Example : "+ autoAllData.distinct().collect());
		System.out.println("Spark Distinct Example : "+ autoAllData.distinct().count());
	
}


}
