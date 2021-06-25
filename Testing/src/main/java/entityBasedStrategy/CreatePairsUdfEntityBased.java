package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.mutable.WrappedArray;

public class CreatePairsUdfEntityBased implements UDF1<WrappedArray<Long>, ArrayList<ArrayList<Long>>> {
	private static final long serialVersionUID = -21621751L;

	public ArrayList<ArrayList<Long>> call(WrappedArray<Long> entities) throws Exception {
		ArrayList<ArrayList<Long>> pairs = new ArrayList<ArrayList<Long>>();

		for (int i = 0; i < entities.length(); i++) {
			Long num1 = entities.apply(i);
			for (int j = i + 1; j < entities.length(); j++) {
				ArrayList<Long> pair = new ArrayList<Long>();
				Long num2 = entities.apply(j);
				pair.add(num1);
				pair.add(num2);
				pairs.add(pair);
				
			}
		}
		return pairs;
	}
}

//class Pairs {
//	long x, y;
//	public Pairs(long x, long y) {
//		this.x = x;
//		this.y = y;
//	}
//}