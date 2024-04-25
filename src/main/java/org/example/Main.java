package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("test").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("src/main/resources/test.txt");

		lines.collect().forEach(System.out::println);


	}
}
