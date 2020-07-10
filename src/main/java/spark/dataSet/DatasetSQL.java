package spark.dataSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetSQL {
    public static void main(String arg[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("TestSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///e:/tmp")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header",true)
                .csv("E:\\work\\Spark\\src\\main\\resources\\students.csv");
        dataset.createOrReplaceTempView("student");

        Dataset<Row> result = spark.sql("select * from student where subject='German' and year = '2007'");

        result.show();
        result.repartition(1);
        Dataset<Row> sub_result = spark.sql("select subject,max(score) from student group by subject");
        sub_result.show();

        Dataset<Row> sub_result_by_num = spark.sql("select subject,count(student_id),avg(score) from student group by subject");
        sub_result_by_num.show();

       // result.coalesce(1).write().option("header",true).csv("test.csv");
        
        spark.close();

    }
}
