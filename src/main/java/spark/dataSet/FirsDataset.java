package spark.dataSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

public class FirsDataset {
    public static void main(String arg[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

//        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder()
                .appName("TestSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///e:/tmp")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header",true)
                .csv("E:\\work\\Spark\\src\\main\\resources\\students.csv");
        Row fistRow =  dataset.first();
        System.out.println("First Row ::::: "+fistRow.toString());

        System.out.println("Column Third "+fistRow.get(3));

        String colname[] = dataset.columns();
        System.out.println("Columns Name  ::::: "+colname);

        System.out.println("Columns Third  Name  ::::: "+dataset.col("subject"));

        System.out.println("Year in Integer = "+Integer.parseInt(fistRow.getAs("year")));

        //dataset.filter("subject=='German'") .show();
//        Dataset<Row> modernArt = dataset.filter(
//                dataset.col("subject").equalTo("Modern Art")
//                .and(dataset.col("year").geq("2007"))
//        );
        Column ModeArt = dataset.col("subject");
        dataset.filter(ModeArt.equalTo("Modern Arts")).show();
        //modernArt.show();
        //&& year == '2005'"
        //dataset.show();


        spark.close();

    }
}
