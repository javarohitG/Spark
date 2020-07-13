package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_8_Csv_TSV {
public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Create a RDD from a file
		JavaRDD<String> autoAllData = sc.textFile("target/My_Data/auto-data.csv");
		System.out.println("Total Records in Autodata : "   + autoAllData.count());
		System.out.println("Spark Operations : Load from CSV");
				

		// Map Example: Change CSV to TSV
		JavaRDD<String> tsvData = autoAllData.map(str -> str.replace(",", "\t"));
		System.out.println("Spark Operations : MAP");
		ExerciseUtils.printStringRDD(tsvData, 5);	
		// Storing RDDs
				// Make sure the file does not exist.
		//autoAllData.saveAsTextFile("target\\My_Data\\data-OUTput11.csv");

				//Print All Data
		       
		//autoAllData.collect().forEach(System.out::println);
		//System.out.print(autoAllData);

		

}


}
