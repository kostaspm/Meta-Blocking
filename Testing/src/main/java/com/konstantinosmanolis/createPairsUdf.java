package com.konstantinosmanolis;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class createPairsUdf implements UDF1<WrappedArray<WrappedArray<Long>>, ArrayList<ArrayList<Long>>> {
	private static Logger log = LoggerFactory.getLogger(createPairsUdf.class);
	private static final long serialVersionUID = -21621751L;

	@Override
	public ArrayList<ArrayList<Long>> call(WrappedArray<WrappedArray<Long>> entities) throws Exception {
		log.debug("-> call({}, {})", entities);
		ArrayList<ArrayList<Long>> pairs = new ArrayList<ArrayList<Long>>();

		for (int i = 0; i < entities.length(); i++) {
			Long num1 = entities.apply(i).apply(0);
			for (int j = i + 1; j < entities.length(); j++) {
				ArrayList<Long> pair = new ArrayList<>();
				Long num2 = entities.apply(j).apply(0);
				pair.add(num1);
				pair.add(num2);
				pairs.add(pair);
				
			}
		}
		return pairs;
	}
}

//class Pairs {
//	long x, y;
//	public Pairs(long x, long y) {
//		this.x = x;
//		this.y = y;
//	}
//}
