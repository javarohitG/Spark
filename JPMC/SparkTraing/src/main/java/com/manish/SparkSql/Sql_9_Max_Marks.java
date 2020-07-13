package com.manish.SparkSql;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Sql_9_Max_Marks {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();

Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");

// Dataset<Row> modernArtResults = dataset.filter("select * from students where subject = 'Modern Art' AND year >= 2007 ");
		
		dataset.createOrReplaceTempView("my_students_table");
		
	//	Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'Modern Art' AND year >= 2007 ");
		
		
	
	
	//Dataset<Row> results = spark.sql("select max(score) from my_students_table where subject = 'Modern Art' ");
	
	//Dataset<Row> results = spark.sql("select avg(score) from my_students_table where subject = 'Modern Art' ");
	
	
	//Dataset<Row> results = spark.sql("select distinct(year) from my_students_table where subject = 'Modern Art' ");
	
	//Order by Year
	Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc ");
	
	
	
	//////////////////////XTRA
	//MAX SCORE FOR EACH SUBJECT
//    Dataset<Row> sqlMaxScorebySubj=spSession.sql("select subject,max(score) max_score from Student_Table group by subject order by max_score desc");
//    System.out.println("Max Score by Subject Row Count:" + sqlMaxScorebySubj.count());
//    sqlMaxScorebySubj.show();
	
	//NOof STUDENTS FOR EACH SUBJ
//
//    Dataset<Row> sqlSubjAgg=spSession.sql("select subject,count(student_id),avg(score) avg_score from Student_Table group by subject ");
//    System.out.println("Count of Students & Avg Score by Subject Row Count:" + sqlSubjAgg.count());
//    sqlSubjAgg.show();

	
	results.show();
		
		spark.close();

	}

}
