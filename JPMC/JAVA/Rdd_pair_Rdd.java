package com.manishSparkJavaspark;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Rdd_pair_Rdd {

	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originallogmsg = sc.parallelize(inputData);
		
		// JavaPairRDD<String, String> myPairRDD = originallogmsg.mapToPair(rawValue -> {
		//	String[] Columns=rawValue.split(":");
			//String level=Columns[0];
			//String date=Columns[1];
	//	return new Tuple2<>(level,date);
		
		///REDUCE BY KEY DEMO
		 JavaPairRDD<String, Long> myPairRDD = originallogmsg.mapToPair(rawValue -> {
				String[] Columns=rawValue.split(":");
				String level=Columns[0];
				
			return new Tuple2<>(level,1L);
			
		} );
		 
		 JavaPairRDD<String, Long> SumRdd= myPairRDD.reduceByKey((value1, value2) -> value1 + value2);
		 
		 SumRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
		sc.close();
	}

}
