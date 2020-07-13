package com.manishSparkJavaspark;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class App_TEST {
	
		public static void main(String args[]) throws InterruptedException {
			
			// Create a session
			SparkSession spark = new SparkSession.Builder()
					.appName("CSV to DB")
					.master("local")
					.getOrCreate();

			// get data
			Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load("src/main/resources/fname_and_review.txt");
		//	df.show();
			
			// Transformation
			
	Dataset<Row> myrdd;
myrdd = df.withColumn("full_name",concat(df.col("last_lname"), lit(", "), df.col("first_name")));
	
	//Another TRANSFORMATION
	
	df=df.filter(df.col("comment").rlike("\\d+"));
	df=df.orderBy(df.col("last_lname").asc());
	
	
	// Write to destination
			String dbConnectionUrl = "jdbc:postgresql://localhost/manish_db"; // <<- You need to create this database
			Properties prop = new Properties();
		    prop.setProperty("driver", "org.postgresql.Driver");
		    prop.setProperty("user", "postgres");
		    prop.setProperty("password", "postgres"); // <- The password you used while installing Postgres
		    
		    df.write()
		    .mode(SaveMode.Overwrite)
	    	.jdbc(dbConnectionUrl, "project3", prop);
	 }
		
}
