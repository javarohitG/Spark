package com.manish.SparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class Sql_17_ExamResults {

	public static void main(String[] args)
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
		
		//Find Out For Each Sub What is Highest Marks  using JAVA API etc..
		
		dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max score") ,
				                                 min(col("score").cast(DataTypes.IntegerType)).alias("min score"));
		
		dataset.show();
	}

}