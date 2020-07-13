package com.manishSparkJavaspark;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class A_First_Pgm {
public static void main(String[] args) {
	Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		//Setup configuration
		String appName = "my_First_App";
		String sparkMaster = "local[2]";
		//String sparkMaster = "spark://169.254.86.212:7077";

		JavaSparkContext spContext = null;
	
		SparkConf conf = new SparkConf()
				.setAppName(appName)
				.setMaster(sparkMaster);
		
		//Create Spark Context from configuration
		spContext = new JavaSparkContext(conf);
		
		//Read a file into an RDD
		JavaRDD<String> tweetsRDD = spContext.textFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\movietweets.csv");
		
		//Print first five lines
		for ( String s : tweetsRDD.take(5)) {
			System.out.println(s);
		}
		
		//Print count.
		System.out.println("Total tweets in file : " + tweetsRDD.count());
		
		/*
		//Convert to upper case
		JavaRDD<String> ucRDD = tweetsRDD.map( str -> str.toUpperCase());
		//Print upper case lines
		for ( String s : ucRDD.take(5)) {
			System.out.println(s);
		}*/
		
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


	}

}

/* Starting your own spark cluster on windows.
 * 
 * Step1 : First go to "SPARK_HOME\bin" Dir and run below commands

spark-class org.apache.spark.deploy.master.Master 
Use the IP and Port provided.

spark-class org.apache.spark.deploy.worker.Worker spark://IP:PORT


*/

