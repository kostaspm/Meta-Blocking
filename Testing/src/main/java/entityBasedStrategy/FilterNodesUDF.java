package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class FilterNodesUDF implements UDF5 <WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Double, ArrayList<Long>> {
	
	private static Logger log = LoggerFactory.getLogger(getFrequencies.class);
	private static final long serialVersionUID = -21621754L;
	
	public ArrayList<Long> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks, Long iEntity, Double NeighborhoodWeight) throws Exception {
		log.debug("-> call({}, {})", frequencies);
		
		ArrayList<Long> filteredEntities = new ArrayList<Long>();
		
		for(int i = 0; i < jEntities.length(); i++){
			
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
			Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
			Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);
			
			
			//Long currentWeight = jaccardScheme(NumberOfBlocks.apply(iEntity.intValue() - 1), NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1), frequencies.apply(jEntities.apply(i).intValue() - 1 ));
			Double currentWeight = (commonBlocks/(iCardinality + jCardinality - commonBlocks));
			if(NeighborhoodWeight < currentWeight) {
				filteredEntities.add(jEntities.apply(i));
				
			}
		}
		
		return filteredEntities;
	}
	
//	private Long jaccardScheme(Long iCardinality, Long jCardinality, Long commonBlocks){
//		Long weight = commonBlocks/(iCardinality + jCardinality - commonBlocks);
//		return weight;
//	}

}

