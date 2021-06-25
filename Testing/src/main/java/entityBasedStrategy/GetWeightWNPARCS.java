package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF2;

import scala.collection.mutable.WrappedArray;

public class GetWeightWNPARCS implements UDF2 <WrappedArray<WrappedArray<Long>>,WrappedArray<Integer>, Double> {
	
	private static final long serialVersionUID = -21621754L;
	
	public Double call(WrappedArray<WrappedArray<Long>> jEntities, WrappedArray<Integer> Cardinality) throws Exception {
		
		Double totalWeight = 0.0;
		
		for(int i = 0; i < jEntities.length(); i++){
			for(int j = 0; j < jEntities.apply(i).length(); j++) {
				double current = 1.0 / (double) (Cardinality.apply(i));
				totalWeight += current;
			}
		}
		
		return totalWeight;
	}
}