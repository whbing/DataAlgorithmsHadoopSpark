package cn.whbing.spark.dataalgorithms.chap02;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.google.common.collect.Lists;

import cn.whbing.spark.dataalgorithms.chap01.util.TupleComparator;
import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

/**
 * 
 * @author whbing
 */

public class Top10 {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SecondarySort by spark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//读取每一行即<cat_id><,><weight>
		JavaRDD<String> lines = sc.textFile("chap02-top10.txt",2);
		//JavaRDD<String> lines = sc.textFile(inputPath);

		//转化成键值对形式-->  <catId,weight>
		JavaPairRDD<String, Integer> pairs = 
				lines.mapToPair(new PairFunction<String,String,Integer>() {

					@Override
					public Tuple2<String, Integer> call(String s) throws Exception {
						//对输入s进行转换
						String[] tokens = s.split(",");//s即cat1,2 以逗号分割
						String catId = tokens[0];
						Integer weight = new Integer(tokens[1]);
						return new Tuple2<String,Integer>(catId,weight);
					}
				});	

		/*
		 * 思路：将RDD以weight为key放入TreeMap中（默认升序），只留后10个即可
		 */
		// STEP-5: create a local top-10
        JavaRDD<TreeMap<Integer, String>> partitions = pairs.mapPartitions(
	         new FlatMapFunction<Iterator<Tuple2<String,Integer>>, TreeMap<Integer, String>>() {
	         @Override
	         public Iterator<TreeMap<Integer, String>> call(Iterator<Tuple2<String,Integer>> iter) {
	             TreeMap<Integer, String> top10 = new TreeMap<Integer, String>();
	             while (iter.hasNext()) {
	                Tuple2<String,Integer> tuple = iter.next();
	                top10.put(tuple._2, tuple._1);
	                // keep only top N 
	                if (top10.size() > 10) {
	                   top10.remove(top10.firstKey());
	                }  
	             }
	             //以下两种均可，map转iterator
	             //return Collections.singletonList(top10).iterator();
	             return Lists.newArrayList(top10).iterator();
	         }
        });
	      
		//验证本地top10
        //当有多个分区时（本测试为2，见input后参数），每个partition都会执行以下操作
        System.out.println("验证本地top10 partitions：");
		partitions.foreach(new VoidFunction<TreeMap<Integer,String>>() {
			
			@Override
			public void call(TreeMap<Integer, String> m) throws Exception {
				for(Map.Entry<Integer, String> entry: m.entrySet()){
					System.out.println(entry.getKey()+":"+entry.getValue());
				}				
			}
		});
		
		//对每个分区的结果汇总
		
		//汇总方案1：collect到集合
		/**
		 *   partition1      partition2
		 *       |               |  
		 *            collect()
		 *               |
		 *      List<partition1或2的类型>
		 *   说明：partition1或2类型一致，List的长度为分区个数，2分区即为2      
		 */
		
		//用于保存最终top10
		TreeMap<Integer,String> finalTop10 = new TreeMap<Integer,String>();
		
		List<TreeMap<Integer, String>> allTop10 = partitions.collect();
		
		for(TreeMap<Integer,String> treeMap : allTop10){  //List
			//每次循环的treeMap为每个分区的treeMap
			//对每个分区的treeMap遍历
			for(Map.Entry<Integer, String> entry : treeMap.entrySet()){
				finalTop10.put(entry.getKey(), entry.getValue());
				//只保留前10
				if(finalTop10.size()>10){
					finalTop10.remove(finalTop10.firstKey());//默认从小到大排序
				}
			}
		}
		//方案1验证finalTop10，打印finalTop10集合即可
		System.out.println("====汇总方案1，collect到集合方法，finalTop10结果:====");
		for(Integer key:finalTop10.keySet()){
			System.out.println(key+" -> "+finalTop10.get(key));
		}
		
		//汇总方案2：reduce到集合
		//用于保存最终top10
		TreeMap<Integer,String> finalTop10_2 = new TreeMap<Integer,String>();
		partitions.reduce(new Function2<
				TreeMap<Integer,String>, 
				TreeMap<Integer,String>, 
				TreeMap<Integer,String>>() {
			
			@Override
			public TreeMap<Integer, String> call(TreeMap<Integer, String> m1, TreeMap<Integer, String> m2) throws Exception {
				//m1,partitionK-1
				for(Map.Entry<Integer, String> entry:m1.entrySet()){
					finalTop10_2.put(entry.getKey(), entry.getValue());
					if(finalTop10_2.size()>10){
						finalTop10_2.remove(finalTop10_2.firstKey());
					}
				}
				//m2,partitionK
				for(Map.Entry<Integer, String> entry:m2.entrySet()){
					finalTop10_2.put(entry.getKey(), entry.getValue());
					if(finalTop10_2.size()>10){
						finalTop10_2.remove(finalTop10_2.firstKey());
					}
				}
				return finalTop10_2;
			}
		});
		//方案2验证finalTop10，打印finalTop10_2集合即可
		System.out.println("====汇总方案2，reduce到集合方法，finalTop10_2结果:====");
		for(Integer key:finalTop10_2.keySet()){
			System.out.println(key+" -> "+finalTop10_2.get(key));
		}
		
	}
}
