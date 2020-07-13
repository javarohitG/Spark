package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark_7_Read_Store_From_File {

public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Create a RDD from a file
		//JavaRDD<String> autoAllData = sc.textFile("target/My_Data/auto-data.csv");
		JavaRDD<String> autoAllData = sc.textFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\auto-data.csv");
		System.out.println("Total Records in Autodata : "   + autoAllData.count());
		System.out.println("Spark Operations : Load from CSV");
				

		
		// Storing RDDs
				// Make sure the file does not exist.
		autoAllData.saveAsTextFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\my_auto-data_OUTPUT.csv");

				//Print All Data
		       
		//autoAllData.collect().forEach(System.out::println);
			//	System.out.print(autoAllData);

		

}

}
