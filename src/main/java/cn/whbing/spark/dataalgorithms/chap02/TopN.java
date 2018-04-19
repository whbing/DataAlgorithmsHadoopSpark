package cn.whbing.spark.dataalgorithms.chap02;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Lists;

import cn.whbing.spark.dataalgorithms.chap01.util.TupleComparator;
import scala.Tuple2;

public class TopN {

	public static void main(String[] args) {
		//读取参数并验证
		if(args.length<1){
			System.err.println("Usage: SecondarySort <file>");
			System.exit(1);  //1表示非正常退出，0表示正常退出
		}
		
		//读取输入的参数，即输入文件
		String inputPath = args[0];
		System.out.println("args[0]:<file>="+args[0]);
		
		//创建sparkConf及sparkContext（集群模式下运行时配模式时sparkConf可以不要）
		SparkConf conf = new SparkConf().setAppName("SecondarySort by spark").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//final JavaSparkContext sc = new JavaSparkContext();
		
		//读取每一行即<name><,><time><,><value>
		//JavaRDD<String> lines = sc.textFile("timeseries.txt");
		JavaRDD<String> lines = sc.textFile(inputPath);
		
		//将每一行的JavaRDD<String>包括<name><,><time><,><value>进行进一步的处理
		//转化成键值对，键是name，值是Tuple2(time,value)
		/*PairFunction<T,K,V> 
		  T-->表示输入
		  K-->表示转化后的key
		  V-->转化后的value
	    */
		/*
		public interface PairFunction<T, K, V> extends Serializable {
  			Tuple2<K, V> call(T t) throws Exception;
		}	 
		*/
		JavaPairRDD<String, Tuple2<Integer,Integer>> pairs = 
				lines.mapToPair(new PairFunction<String, String, Tuple2<Integer,Integer>>() {

					@Override
					public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
						//对输入s进行转换
						String[] tokens = s.split(",");//s即x,2,5 以逗号分割
						System.out.println(tokens[0]+","+tokens[1]+","+tokens[2]);
						Integer time = new Integer(tokens[1]);//parseInt,valueOf都可以
						Integer value = new Integer(tokens[2]);
						Tuple2<Integer,Integer> timevalue = new Tuple2<Integer,Integer>(time,value);
						return new Tuple2<String,Tuple2<Integer, Integer>>(tokens[0],timevalue);
					}
				});	
		//验证上述中间RDD的结果
		List<Tuple2<String, Tuple2<Integer,Integer>>> out1 =  
				pairs.take(5);//collect
		System.out.println("==DEBUG1==");
		System.out.println("JavaPairRDD collect as:");
		for(Tuple2 t:out1){
			System.out.println(t._1+","
					+((Tuple2<Integer, Integer>)t._2)._1+","
					+((Tuple2<Integer, Integer>)t._2)._2);
		}
		
		//对name进行分组groupByKey，分组之后相同key显示一个，value汇集在一起为Iterable
		JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> groups = 
				pairs.groupByKey();
		
		//对groups进行验证,使用collect收集结果，存为List，里面PairRDD记为Tuple2型
		System.out.println("==DEBUG2==");
		System.out.println("groupByKey as:");
		List<Tuple2<String,Iterable<Tuple2<Integer,Integer>>>> out2 = 
				groups.collect();
		for(Tuple2<String,Iterable<Tuple2<Integer,Integer>>> t:out2){
			System.out.println(t._1);
			Iterable<Tuple2<Integer,Integer>> list = t._2;
			for(Tuple2<Integer,Integer> t2:list){
				System.out.println(t2._1+","+t2._2);
			}
			System.out.println("----");
		}
		
		//groups中的value是Iterable类型，每一条数据是Tuple2，需要对其排序
		//比较器件util.TupleComparator
		//new Function参数，前一个是输入，后一个是输出
		JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> sorted = 
				groups.mapValues(new Function<Iterable<Tuple2<Integer,Integer>>, 
						Iterable<Tuple2<Integer,Integer>>>() {

					@Override
					public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v)
							throws Exception {
						
						//直接对v排序是错误的，因为RDD不可变，要复制一份
						//Collections.sort(v, new TupleComparator());
						
						//书中有错误，需要转化v成ArrayList，不能直接是iterable
						//以下转化也有问题，引入google的包
						//ArrayList<Tuple2<Integer, Integer>> listOut = new ArrayList<>((ArrayList)v);

						ArrayList<Tuple2<Integer, Integer>> listOut =Lists.newArrayList(v);
						
						Collections.sort(listOut,TupleComparator.INSTANCE);//new TupleComparator()
						return listOut; //listOut属于Iterable，可以返回
						
					}				
		});

		//输出sorted
		System.out.println("==DEBUG OUTPUT==");
		List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> out = 
				sorted.collect();
		for(Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t:out){
			System.out.println(t._1);//key
			for(Tuple2<Integer, Integer> t2:t._2){
				System.out.println(t2._1+","+t2._2);
			}
			System.out.println("----");
		}
	}
}
