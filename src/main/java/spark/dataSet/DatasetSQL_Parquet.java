package spark.dataSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
public class DatasetSQL_Parquet {
    public static void main(String arg[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("TestSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///e:/tmp")
                .getOrCreate();
        Dataset<Row> baby = spark.read().parquet("E:\\work\\Spark\\src\\main\\resources\\baby_names.parquet");

        baby.show();
        baby.printSchema();

        //Do data frame queries
        System.out.println("SELECT Demo :");
        baby.select(col("name"),col("prob")).show();


        spark.close();

    }
}
