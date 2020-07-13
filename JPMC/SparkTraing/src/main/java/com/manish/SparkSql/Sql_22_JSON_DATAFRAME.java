package com.manish.SparkSql;

import java.util.ArrayList;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Sql_22_JSON_DATAFRAME {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) 
	{
		//System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
				                                   .getOrCreate();
		
				
		Dataset<Row> empDf = spark.read().json("target/My_Data/customerData.json");
		empDf.show();
		empDf.printSchema();
		
		
		//Do data frame queries
				System.out.println("SELECT Demo :");
				empDf.select(col("name"),col("salary")).show();
				
				System.out.println("FILTER for Age == 40 :");
				empDf.filter(col("age").equalTo(40)).show();
				
				System.out.println("GROUP BY gender and count :");
				empDf.groupBy(col("gender")).count().show();
				
				System.out.println("GROUP BY deptId and find average of salary and max of age :");
				Dataset<Row> summaryData = empDf.groupBy(col("deptid"))
					.agg(avg(empDf.col("salary")), max(empDf.col("age")));
				summaryData.show();
				
				
				//Create Dataframe from a list of objects
				Department dp1 = new Department("100","Sales");
				Department dp2 = new Department("200","Engineering");
				List<Department> deptList = new ArrayList<Department>();
				deptList.add(dp1);
				deptList.add(dp2);
				
				Dataset<Row> deptDf = spSession
		            .createDataFrame(deptList, Department.class);
				System.out.println("Contents of Department DF : ");
				deptDf.show();
				
				System.out.println("JOIN example :");
				Dataset<Row> joinDf = empDf.join(deptDf,
		                            col("deptid").equalTo(col("id")));
				joinDf.show();
				
				System.out.println("Cascading operations example : ");
				empDf.filter( col("age").gt(30) )
						.join(deptDf,col("deptid").equalTo(col("id")) )
						.groupBy(col("deptid"))
						.agg(avg(empDf.col("salary")), max(empDf.col("age"))).show();
}
	
}
