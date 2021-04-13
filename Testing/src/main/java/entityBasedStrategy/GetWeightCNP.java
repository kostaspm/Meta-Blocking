package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class GetWeightCNP implements UDF4 <WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, ArrayList<Double>> {
	
	private static Logger log = LoggerFactory.getLogger(getFrequencies.class);
	private static final long serialVersionUID = -21621754L;
	
	public ArrayList<Double> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks, Long iEntity) throws Exception {
		log.debug("-> call({}, {})", frequencies);
		
		ArrayList<Double> weights = new ArrayList<Double>();
		
		for(int i = 0; i < jEntities.length(); i++){
			
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
			Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
			Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);
			
			
			//Long currentWeight = jaccardScheme(NumberOfBlocks.apply(iEntity.intValue() - 1), NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1), frequencies.apply(jEntities.apply(i).intValue() - 1 ));
			double currentWeight = jaccardScheme(iCardinality, jCardinality, commonBlocks);
			weights.add(currentWeight);
			
		}
		
		return weights;
	}
	
	private double jaccardScheme(Double iCardinality, Double jCardinality, Double commonBlocks){
		double weight = commonBlocks/(iCardinality + jCardinality - commonBlocks);
		return weight;
	}

}

