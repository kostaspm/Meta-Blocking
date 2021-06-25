package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class GetWeightWNPJaccard implements UDF4 <WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Double> {
	
	private static final long serialVersionUID = -21621754L;
	
	public Double call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks, Long iEntity) throws Exception {
		
		Double totalWeight = 0.0;
		
		for(int i = 0; i < jEntities.length(); i++){
			
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
			Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
			Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);
			
			
			//Long currentWeight = jaccardScheme(NumberOfBlocks.apply(iEntity.intValue() - 1), NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1), frequencies.apply(jEntities.apply(i).intValue() - 1 ));
			Double currentWeight = (commonBlocks/(iCardinality + jCardinality - commonBlocks));
			totalWeight += currentWeight;
		}
		
		return totalWeight;
	}
	
//	private Long jaccardScheme(Long iCardinality, Long jCardinality, Long commonBlocks){
//		Long weight = commonBlocks/(iCardinality + jCardinality - commonBlocks);
//		return weight;
//	}

}

