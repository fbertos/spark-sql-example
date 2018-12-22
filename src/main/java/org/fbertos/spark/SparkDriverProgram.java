package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;


public class SparkDriverProgram {
    @SuppressWarnings({ "unchecked", "deprecation" })
	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
    	String file_path = "/home/fbertos/workspace/spark-sql-example/apache-log04-aug-31-aug-2011-nasa.log";
    	String ip_path = "/home/fbertos/workspace/spark-sql-example/ip-nasa.log";
        SparkConf conf = new SparkConf().setAppName("spark-sql-example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
		SQLContext sqlContext = new SQLContext(sc);
		
		JavaRDD<Entry> file = sqlContext.read().textFile(file_path).javaRDD().map(line -> {
			String[] tmp = line.split(" ");
			Entry entry = new Entry(tmp[0], tmp[3].replace("[", ""));
			return entry;
		});
		
		Dataset<Row> table = sqlContext.createDataFrame(file, Entry.class);
		
		table.createOrReplaceTempView("entries");
		
		JavaRDD<IP> ip_file = sqlContext.read().textFile(ip_path).javaRDD().map(line -> {
			String[] tmp = line.split(";");
			IP ip = new IP(tmp[0], tmp[1]);
			return ip;
		});
		
		Dataset<Row> ip_table = sqlContext.createDataFrame(ip_file, IP.class);
		
		ip_table.createOrReplaceTempView("ips");
		
		Dataset <Row> results = table.join(ip_table, "ip");
		
		Dataset<String> ds = results.map(
				(MapFunction<Row, String>) row -> row.getString(2),
				Encoders.STRING());		
		
		/*
		Dataset<Row> results = sqlContext.sql("SELECT ip FROM entries");
			
		Dataset<String> ds = results.map(
				(MapFunction<Row, String>) row -> row.getString(0) + " " + row.getString(1),
				Encoders.STRING());		
		*/
		
		/*
		Dataset<Row> results = sqlContext.sql("SELECT max(date), min(date) FROM entries");
		

		Dataset<Row> results = sqlContext.sql("SELECT max(date), min(date) FROM entries group by ip");
		
		Dataset<String> ds = results.map(
				(MapFunction<Row, String>) row -> row.getString(0) + " " + row.getString(1),
				Encoders.STRING());
		*/
		
		//Dataset <Row> joined = dfairport.join(dfairport_city_state, dfairport_city_state("City"), "left_outer");


		/*
		Dataset<Row> results = sqlContext.sql("SELECT count(*) FROM entries where ip = '170.183.124.101'");
		
		
		Dataset<Long> ds = results.map(
				(MapFunction<Row, Long>) row -> row.getLong(0),
				Encoders.LONG());
		*/
		
		
		ds.show(false);
		
	
        sc.close();
      
    }
    
}