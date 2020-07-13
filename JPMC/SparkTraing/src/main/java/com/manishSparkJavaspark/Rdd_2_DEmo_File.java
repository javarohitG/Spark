package com.manishSparkJavaspark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_2_DEmo_File {
		    public static void main(String args[]) throws InterruptedException {

	        Logger.getLogger("org.apache").setLevel(Level.WARN);

	        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);



	        JavaRDD<String> readFile = sc.textFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\auto-data.csv");
	        System.out.println("Count is " + readFile.count());
	        

	        sc.close();


	    }

	}
