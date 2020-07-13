package com.manishSparkJavaspark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Tuple_3_Demo {
	public static void main(String args[]) throws InterruptedException {
		
		List<Integer> input_data=new ArrayList<>();
		input_data.add(20);
		
		input_data.add(21);
		input_data.add(22);
		input_data.add(23);
		input_data.add(24);
		input_data.add(25);
		
      Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> originalIntegers = sc.parallelize(input_data);
		
		JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map( value -> new Tuple2<>(value, Math.sqrt(value)) );
		
		//new Tuple5(4, 3, 2, 1, 2);
		
		//new Tuple23(); #Max Limit 22
		
		//SqrtRdd.collect().forEach(System.out::println);
	//	System.out.print(SqrtRdd);
		sc.close();
	}

}
