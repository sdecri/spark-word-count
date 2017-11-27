/**
 * WordCount.java
 */
package com.sdc.spark.word_count;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Simone De Cristofaro
 * Nov 24, 2017
 */
public class WordCount {

    public static void main(String[] args) throws IOException {
        
        String input = args[0];
        String output = args[1];
        
        int cores = 8;
        
        SparkConf sc = new SparkConf()
                .setMaster(String.format("local[%s]", cores))
                .setAppName("word-count");
        try(JavaSparkContext jsc = new JavaSparkContext(sc)){
            
            JavaPairRDD<String, Integer> wordCountRdd = 
                    jsc.textFile(input, cores * 2)
            .flatMap(line -> Arrays.asList(line.trim().split("\\s+")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((count1, count2) -> count1 + count2)
            ;
            wordCountRdd.repartition(1).saveAsTextFile(output);
            
            manualEnd();
        }
        
    }

    /**
     * @throws IOException
     */
    private static void manualEnd() throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        System.out.print("Please press ENTER botton to close application...");
        reader.readLine();
    }
    
}
