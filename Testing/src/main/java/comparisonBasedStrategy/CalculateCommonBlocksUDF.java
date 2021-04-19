package comparisonBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.mutable.WrappedArray;

public class CalculateCommonBlocksUDF implements UDF1 <WrappedArray<WrappedArray<Long>>, ArrayList<Long>> {

	private static final long serialVersionUID = -216217592L;
	
	public ArrayList<Long> call(WrappedArray<WrappedArray<Long>> associatedBlocks) throws Exception {
		ArrayList<Long> commonBlocks = new ArrayList<Long>();
		for(int i = 0; i < associatedBlocks.length(); i++){
			WrappedArray<Long> array1 = associatedBlocks.apply(i);
			for (int j = i + 1; j < associatedBlocks.length(); j++){
				WrappedArray<Long> array2 = associatedBlocks.apply(j);
				WrappedArray<Long> intersect = (WrappedArray<Long>) array1.intersect(array2);
				commonBlocks.add((long) intersect.length());
			}
		}
		return commonBlocks;
	}
}
