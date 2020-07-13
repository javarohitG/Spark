package com.manish.SparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Sql_4_Filter_with_Lambda {
	
	public static void main(String args[]) throws InterruptedException {
		
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
	//RDD Method to Ceate Spark Contetx
	//	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
	//	JavaSparkContext sc = new JavaSparkContext(conf);
		
//****************************//
///SPARK SQL We Need SPARK SESSION Obj *********** ??????////
		
SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();


//spark.read ---> See All Options-- read csv,json, txt , jdbc  etc...

Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");

///I Want Only From Yr 2007 Onwards 

//Dataset<Row> filtered= dataset.filter("subject='Modern Art' AND year >=2007");

//// LAMBDA EXPRESSION Like RDD 

Dataset<Row> modernArtResults = dataset.filter(Row->Row.getAs("subject").equals("Modern Art")
		 && Integer.parseInt(Row.getAs("year"))>2007);

///Above Both Methods are Same 	

//Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
//		.and(col("year").geq(2007)));

modernArtResults.show();

spark.close();

}
	
}
