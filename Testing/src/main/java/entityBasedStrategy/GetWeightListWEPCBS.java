package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF5;

import scala.collection.mutable.WrappedArray;

public class GetWeightListWEPCBS implements UDF5 <WrappedArray<Long>, WrappedArray<Long>, Long, Double, Long, ArrayList<ArrayList<Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8618285765926468296L;

	public ArrayList<ArrayList<Double>> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, Long iEntity, Double totalWeight, Long numberOfEdges) throws Exception {
		
		ArrayList<Double> weightList = new ArrayList<Double>();
		ArrayList<ArrayList<Double>> list = new ArrayList<ArrayList<Double>>();
		
		for(int i = 0; i < jEntities.length(); i++){
			
			if(jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1 );
				
				double currentWeight = commonBlocks;
				double finalWeight = totalWeight/numberOfEdges;
				if(currentWeight > finalWeight) {
					weightList.add((double)jEntities.apply(i));
					weightList.add(currentWeight);
					list.add(weightList);
				}
			}
		}
		
		return list;
	}
}

