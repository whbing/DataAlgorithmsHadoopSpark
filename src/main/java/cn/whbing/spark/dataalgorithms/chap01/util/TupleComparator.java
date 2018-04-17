package cn.whbing.spark.dataalgorithms.chap01.util;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class TupleComparator implements 
				Comparator<Tuple2<Integer,Integer>>,Serializable {

	public static final TupleComparator INSTANCE = new TupleComparator(); 
	
	@Override
	public int compare(Tuple2<Integer, Integer> t1, 
			Tuple2<Integer, Integer> t2) {
		if(t1._1 <t2._1){
			return -1;
		}else if(t1._1 >t2._1){
			return 1;
		}
		return 0;
	}

}
