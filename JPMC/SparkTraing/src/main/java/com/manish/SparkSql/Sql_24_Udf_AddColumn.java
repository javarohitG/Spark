/// ADD a Column Pass /Fail 

package com.manish.SparkSql;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;


public class Sql_24_Udf_AddColumn {
	
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
			
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");
      
		
		dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")) );
		
		dataset.show();
}


}

/// But In REal Scenerio we gets much more Complex Job To Do  so Introducting UDF...

