package com.manish.SparkSql;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Sql_6_Filter_Columns {
	
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


Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");


//Column subjectColumn= dataset.col("subject"); ///column import from org.apache.spark.sql
//Column YearColumn= dataset.col("year");
//NOw We can Use This Obje cin Expression

//Dataset<Row> filtered= dataset.filter(subjectColumn.equalTo("Modern Art")
	//	.and(YearColumn.geq(2007)));


// THis Style is Better than Convention 
//Dataset<Row> filtered= dataset.filter("subject='Modern Art' AND year >=2007");/ Specially for Complex Queries

//filtered.show();


// Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");

Dataset<Row> modernArtResults = dataset.filter(column("subject").equalTo("Modern Art")
															.and(column("year").geq(2007)));

modernArtResults.show();


spark.close();

}

}
