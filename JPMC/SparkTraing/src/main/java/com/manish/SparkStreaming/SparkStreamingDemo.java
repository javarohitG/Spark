package com.manish.SparkStreaming;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import com.manish.SparkStreaming.ExerciseUtils;
//import com.manish.SparkStreaming.SparkConnection;

import scala.Tuple2;

public class SparkStreamingDemo {
	public static void main(String[] args) {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Spark_Streaming").setMaster("local[2]");
			
		JavaStreamingContext streamingContext 
		= new JavaStreamingContext(conf, Durations.seconds(3));
	
	JavaReceiverInputDStream<String> lines 
		= streamingContext.socketTextStream("localhost", 9000);
	
	lines.print();
	// Split each line into words
	JavaDStream<String> words = lines.flatMap(
	  new FlatMapFunction<String, String>() {
	    @Override public Iterator<String> call(String x) {
	      return Arrays.asList(x.split(" ")).iterator();
	    }
	  });
	
	// Count each word in each batch
	JavaPairDStream<String, Integer> pairs = words.mapToPair(
	  new PairFunction<String, String, Integer>() {
	    @Override public Tuple2<String, Integer> call(String s) {
	      return new Tuple2<>(s, 1);
	    }
	  });
	JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
	  new Function2<Integer, Integer, Integer>() {
	    @Override public Integer call(Integer i1, Integer i2) {
	      return i1 + i2;
	    }
	  });

	// Print the first ten elements of each RDD generated in this DStream to the console
	
	wordCounts.print();
	
//	Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
//		  @Override public Integer call(Integer i1, Integer i2) {
//		    return i1 + i2;
//		  }
//		};
//		
//	JavaPairDStream<String, Integer> windowedWordCounts 
//		= pairs.reduceByKeyAndWindow(reduceFunc,
//									Durations.seconds(15), 
//									Durations.seconds(3));
//	
//	windowedWordCounts.print();
//	
//	System.out.println("Starting Streaming...");
	
	
	streamingContext.start();
	
	try {
		streamingContext.awaitTermination();
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
	
	streamingContext.close();
	
	ExerciseUtils.hold();
}
}
