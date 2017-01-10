package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Spark1Application {


	public static void main(String[] args) {
		SpringApplication.run(Spark1Application.class, args);

		String appName = "Spark3";
        String sparkMaster = "local[*]";
//        String sparkMaster = "spark://169.254.86.212:7077";
        JavaSparkContext sc = null;


        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(sparkMaster);

        sc = new JavaSparkContext(conf);

        JavaRDD<String> tweetsRDD = sc.textFile("data/movietweets.csv");

        for (String s : tweetsRDD.take(5) ) {
            System.out.println(s);
        }

        JavaRDD<String> upRDD = tweetsRDD.map(s -> s.toUpperCase());

        for (String s : upRDD.take(5)) {

            System.out.println(s);
        }

        System.out.println("Total tweets in file : " + tweetsRDD.count());

        while(true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

	}
}
