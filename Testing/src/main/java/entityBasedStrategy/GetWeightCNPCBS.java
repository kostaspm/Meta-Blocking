package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class GetWeightCNPCBS implements UDF2 <WrappedArray<Long>, WrappedArray<Long>, ArrayList<Double>> {
	
	private static Logger log = LoggerFactory.getLogger(getFrequencies.class);
	private static final long serialVersionUID = -21621754L;
	
	public ArrayList<Double> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities) throws Exception {
		log.debug("-> call({}, {})", frequencies);
		
		ArrayList<Double> weights = new ArrayList<Double>();
		
		for(int i = 0; i < jEntities.length(); i++){
			
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
			
			double currentWeight = commonBlocks;
			weights.add(currentWeight);
			
		}
		
		return weights;
	}
}

