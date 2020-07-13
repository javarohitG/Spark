package com.manishSparkJavaspark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class A__Word_Count  {

	public static void main(String args[]) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

       // JavaRDD<String> inputFile = sc.textFile("C:\\Users\\Manish\\Desktop\\spark_Eclipse\\SparkTraing\\target\\My_Data\\hello_world.txt");
        JavaRDD<String> inputFile = sc.textFile("s3://https://manishteachers3.s3.amazonaws.com//hello_world.txt");
       JavaRDD<String> words = inputFile.flatMap( line -> Arrays.asList(line.split(" ")).iterator());
       		
       		JavaPairRDD<String, Integer> pairs = words.mapToPair( word -> new scala.Tuple2<String, Integer>(word, 1));
       		
       		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b );
       				
      		counts.saveAsTextFile("target\\My_Data\\word_Count_Output1");
      		
    		sc.close();
       		
       	}
       }