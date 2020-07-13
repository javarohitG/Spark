package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Rdd_Demo_1 {
	public static void main(String args[]) throws InterruptedException {
		
		List<Double> input_data=new ArrayList<>();
		input_data.add(20.5);
		
		input_data.add(21.5);
		input_data.add(22.5);
		input_data.add(23.5);
		input_data.add(24.5);
		input_data.add(25.5);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
	JavaRDD<Double> myRdd = sc.parallelize(input_data);
	Double result=myRdd.reduce((value1,value2)-> value1 +value2);
	System.out.print(result);
	sc.close();
	
	}

}
	
	
