package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF4;

import scala.collection.mutable.WrappedArray;

public class GetTotalWeightWEP implements UDF4 <WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Double> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7469620826868963887L;

	public Double call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks, Long iEntity) throws Exception {
		
		double totalWeight = 0.0;
		
		for(int i = 0; i < jEntities.length(); i++){
			
			if(jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
				Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
				Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);
				
				double currentWeight = jaccardScheme(iCardinality, jCardinality, commonBlocks);
				totalWeight += currentWeight;
			}
		}
		
		return totalWeight;
	}
	
	private double jaccardScheme(Double iCardinality, Double jCardinality, Double commonBlocks){
		double weight = commonBlocks/(iCardinality + jCardinality - commonBlocks);
		return weight;
	}

}

