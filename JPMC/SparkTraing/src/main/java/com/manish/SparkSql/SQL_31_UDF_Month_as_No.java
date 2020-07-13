///Convert Month Name into a No... 
package com.manish.SparkSql;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class SQL_31_UDF_Month_as_No {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		spark.udf().register("monthNum", (String month) -> {
			java.util.Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		}, DataTypes.IntegerType);
				
		dataset.createOrReplaceTempView("logging_table");
		
		
		Dataset<Row> results = spark.sql
		  ("select level, date_format(datetime,'MMMM') as month, count(1) as total " + 
		   "from logging_table group by level, month order by monthNum(month), level");			
		
	
	
		results.show(100);
			
		spark.close();
	}

}


