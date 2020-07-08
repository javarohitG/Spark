package spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import static org.apache.commons.lang3.StringUtils.isNumeric;

public class Rdd_MPG {
	public static void main(String args[]) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Create a RDD from a file
		JavaRDD<String> autoAllData = sc.textFile("E:\\work\\Spark\\src\\main\\resources\\auto-data.csv");
		System.out.println("Total Records in Autodata :" + autoAllData.count());
		System.out.println("Spark Operations : Load from CSV");

		//Remove first header line
		String header = autoAllData.first();
		JavaRDD<String> autoData = autoAllData.filter(s -> !s.equals(header));

		String totMPG = autoData.reduce(new totalMPG());
		System.out.println("Average MPG is " + (Integer.valueOf(totMPG) / (autoData.count() )));
	}
}
class totalMPG implements Function2<String, String, String> {
	@Override
	public String call(String s1, String s2) throws Exception {
		int v1 = 0;
		int v2 = 0;

		v1 = (isNumeric(s1) ? Integer.valueOf(s1) : getMPGValue(s1));
		v2 = (isNumeric(s2) ? Integer.valueOf(s2) : getMPGValue(s2));

		return Integer.valueOf(v1 + v2).toString();
	}

	private int getMPGValue(String str) {
		// System.out.println(str);
		String[] attList = str.split(",");
		if (isNumeric(attList[9])) {
			return Integer.valueOf(attList[9]);
		} else {
			return 0;
		}
	}

	private boolean isNumeric(String s) {
		return s.matches("[-+]?\\d*\\.?\\d+");
	}
}