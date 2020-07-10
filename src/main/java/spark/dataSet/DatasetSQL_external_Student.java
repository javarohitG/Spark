package spark.dataSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class DatasetSQL_external_Student {
    public static void main(String arg[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("TestSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///e:/tmp")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header",true)
                .csv("E:\\work\\Spark\\src\\main\\resources\\students.csv");
        dataset = dataset.select(col("subject"),col("score").cast(DataTypes.IntegerType));

        dataset = dataset.groupBy(col("subject")).max("score");
        dataset.show();
        spark.close();

    }
}
