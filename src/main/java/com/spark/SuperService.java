package com.spark;


import org.apache.spark.api.java.JavaRDD;
import org.springframework.stereotype.Service;

import java.io.Serializable;

@Service
public class SuperService implements Serializable {


    public int sumWords(JavaRDD<String> text) {
        return (int) text.map(String::toLowerCase)
                .flatMap(WordsUtil::getWords).count();


    }

}
