package spark.dataSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

public class DatasetSQL_JSON {
    public static void main(String arg[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("TestSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///e:/tmp")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().json("src\\main\\resources\\customerData.json");

        dataset.createOrReplaceTempView("employee");

        dataset.show();
        dataset.printSchema();

        Dataset<Row> summaryData_sql = spark.sql("select deptid, avg(salary), max(age) from employee group by deptid  ");
        summaryData_sql.show();


        spark.close();

    }
}
