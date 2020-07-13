package com.manishSparkJavaspark;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class C_1_key_pair {
	public static void main(String args[]) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		JavaRDD<String> initialRdd = sc.textFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\resources\\subtitles\\input.txt");
		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );
		
JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0 );
		
	//	JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		List<String> results =  removedBlankLines.take(10000);
		results.forEach(System.out::println);


 }
	
}
