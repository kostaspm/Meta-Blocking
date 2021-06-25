package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class GetWeightWNPCommonBlocks implements UDF2 <WrappedArray<Long>, WrappedArray<Long>, Double> {
	
	private static final long serialVersionUID = -21621754L;
	
	public Double call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities) throws Exception {
		
		Double totalWeight = 0.0;
		
		for(int i = 0; i < jEntities.length(); i++){
			
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
			Double currentWeight = commonBlocks;
			totalWeight += currentWeight;
		}
		
		return totalWeight;
	}
	
//	private Long jaccardScheme(Long iCardinality, Long jCardinality, Long commonBlocks){
//		Long weight = commonBlocks/(iCardinality + jCardinality - commonBlocks);
//		return weight;
//	}

}

