package cn.whbing.spark.dataalgorithms.chap02;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
/**
 * 本测试方法主要对比map()与flatap()的区别
 * @author whbing
 *
 */
public class TestMethod {

	public static void main(String[] args) {
				
		SparkConf conf = new SparkConf().setAppName("SecondarySort by spark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		

		JavaRDD<String> lines = sc.textFile("chap02-testMethod.txt");
		
		//测试map和flatMap
		
		//1.map *2 String->String
		JavaRDD<String> mapRes = lines.map(new Function<String, String>() {
			@Override
			public String call(String s) throws Exception {
				String[] tokens = s.split(",");//1,2,3
				return new Integer(tokens[0])*2 +","+ new Integer(tokens[1])*2+","+ new Integer(tokens[2])*2;
			}
		});
		//验证map
		System.out.println("map:*2");

		mapRes.foreach(new VoidFunction<String>() {					
			@Override			
			public void call(String t) throws Exception {
				System.out.println(t);//打印了两行，证明每一行是一个RDD
			}
		});
		
		//2.flatMap *2 String->Iterable(String)
		JavaRDD<String> flatMapRes = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String s) throws Exception {
				String[] tokens = s.split(",");//1,2,3
				String[] r = new String[]{Integer.valueOf(tokens[0])*2+"",Integer.valueOf(tokens[1])*2+"",Integer.valueOf(tokens[2])*2+""};
				return Arrays.asList(r).iterator();
			}
		});
		//验证flatMap
		System.out.println("flatMap: *2");
		flatMapRes.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);//打印了6行，证明有6个RDD
			}
		});
		
		//3.测试mapPartitions
		JavaRDD<String> mapPar = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			@Override
			public Iterator<String> call(Iterator<String> it) throws Exception {

				List<String> list = new ArrayList();
				
				//输入为iterable
				//在这里并不会执行，因为单纯操作算法并不会执行
				//System.out.println("打印mapPartitions接受的iterator数据，每行为一个it.next()");
				
				//代码1：输入为整个partition，每一个iterator为每一行，不是行内
				//每一行操作
				while(it.hasNext()){
					String s = it.next();					
					list.add(s+"test"); //每一行后边+test
					//1,2,3test
					//88,110,132test
				}
				//代码2：不用迭代不是处理的每一行，仅是第一个元素
				//list.add(it.next()+"test");//仅第一个元素即第一行加test
				//1,2,3test
				return list.iterator();
			}
		});
		//验证mapPartitions
		System.out.println("打印mapPartitions后数据:");
		mapPar.foreach(new VoidFunction<String>() {

			@Override
			public void call(String s) throws Exception {
				System.out.println(s);				
			}
			
		});
		
	}

}
