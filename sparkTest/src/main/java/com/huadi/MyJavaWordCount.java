package com.huadi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/***
 * github 提交测试
 */
public class MyJavaWordCount {
    public static void main(String[] args) {
        //参数检查
        if(args.length<2){
            System.err.println("Usage: MyJavaWordCount <input> <output> ");
            System.exit(1);
        }
        //获取参数
        String input=args[0];
        String output=args[1];
        //创建 java 版本的 SparkContext,local 为本地运行模式
        SparkConf conf = new SparkConf().setAppName("MyJavaWordCount");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //读取数据
        JavaRDD<String> inputRdd=sc.textFile(input);
        //进行相关计算
        JavaRDD<String> words=inputRdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> result=words.mapToPair(new PairFunction<String, String,
                Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                return x+y;
            }

        });
        //保存结果
        result.saveAsTextFile(output);
        //关闭 sc
        sc.stop();
    } }
