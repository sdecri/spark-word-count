/**
 * WordCount.java
 */
package com.sdc.spark.word_count;


/**
 * @author Simone De Cristofaro
 * Nov 24, 2017
 */
public class WordCount {

    public static void main(String[] args) {
        
        String target = "output";
        if(args.length > 0)
            target = args[0];
        System.out.println(target);
        
    }
    
}
