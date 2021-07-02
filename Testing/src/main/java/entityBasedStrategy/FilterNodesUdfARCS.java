package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF4;

import scala.collection.mutable.WrappedArray;

public class FilterNodesUdfARCS implements UDF4<WrappedArray<Long>, WrappedArray<Long>,WrappedArray<Double>, Double, ArrayList<ArrayList<Double>>> {
	private static final long serialVersionUID = -21621751L;

	public ArrayList<ArrayList<Double>> call(WrappedArray<Long> iEntities, WrappedArray<Long> jEntities, 
			WrappedArray<Double> weights, Double meanWeight) throws Exception {
		
		ArrayList<ArrayList<Double>> pairs = new ArrayList<ArrayList<Double>>();
		
		for(int i = 0; i < iEntities.length(); i++) {
			ArrayList<Double> triple = new ArrayList<Double>();
			if(weights.apply(i) > meanWeight) {
				triple.add( (double)iEntities.apply(i));
				triple.add( (double)jEntities.apply(i));
				triple.add( (double)weights.apply(i));
				pairs.add(triple);
			}
			
		}
		
		return pairs;
	}
}