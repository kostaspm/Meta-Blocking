package com.konstantinosmanolis;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class createPairsUdfTest implements UDF1<WrappedArray<Long>, ArrayList<long[]>> {
	private static Logger log = LoggerFactory.getLogger(createPairsUdfTest.class);

	private static final long serialVersionUID = -21621750L;

	@Override
	public ArrayList<long[]> call(WrappedArray<Long> entities) throws Exception {
		log.debug("-> call({}, {})", entities);
		
		ArrayList<long[]> pairs = new ArrayList<long[]>();
		Combination.printCombination(entities, entities.length(), 2, pairs);

		return pairs;
	}

}

class Combination {

	/*
	 * arr[] ---> Input Array data[] ---> Temporary array to store current
	 * combination start & end ---> Staring and Ending indexes in arr[] index
	 * ---> Current index in data[] r ---> Size of a combination to be printed
	 */
	/*
	 * arr[] ---> Input Array data[] ---> Temporary array to store current
	 * combination start & end ---> Staring and Ending indexes in arr[] index
	 * ---> Current index in data[] r ---> Size of a combination to be printed
	 */
	static void combinationUtil(WrappedArray<Long> arr, long[] data, int start, int end, int index, int r, ArrayList<long[]> pairs) {
		// Current combination is ready to be stored
		if (index == r) {
			long[] combination = data.clone();
			pairs.add(combination);
			return;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
			data[index] = arr.apply(i);
			combinationUtil(arr, data, i + 1, end, index + 1, r, pairs);
		}
	}

	// The main function that prints all combinations of size r
	// in arr[] of size n. This function mainly uses combinationUtil()
	static void printCombination(WrappedArray<Long> arr, int n, int r, ArrayList<long[]> pairs) {
		// A temporary array to store all combination one by one
		long[] data = new long[r];
		//ArrayList<Long> data = new ArrayList<Long>(r);

		// Print all combination using temporary array 'data[]'
		combinationUtil(arr, data, 0, n - 1, 0, r, pairs);
	}
}