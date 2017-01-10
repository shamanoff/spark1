package com.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class Spark1ApplicationTests {

    @Autowired
    private SuperService superService;

    @Autowired
    private JavaSparkContext sc;
    @Test
    public void textLoad() throws Exception{

        List<String> list = Arrays.asList("Java the java", "the java the");
        JavaRDD<String> rdd = sc.parallelize(list);
        int sum = superService.sumWords(rdd);
        Assert.assertEquals(6, sum);
    }

}

