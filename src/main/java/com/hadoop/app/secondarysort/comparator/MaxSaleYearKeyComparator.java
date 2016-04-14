package com.hadoop.app.secondarysort.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;

public class MaxSaleYearKeyComparator extends WritableComparator{

	protected MaxSaleYearKeyComparator() {
		super(LocationMaxSaleYearPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable writableComparable1, WritableComparable writableComparable2) {
		LocationMaxSaleYearPair locationMaxSaleYearPair1 = (LocationMaxSaleYearPair)writableComparable1;
		LocationMaxSaleYearPair locationMaxSaleYearPair2 = (LocationMaxSaleYearPair)writableComparable2;
		int cmp = 0;
		return (cmp = locationMaxSaleYearPair1.getLocation().compareTo(locationMaxSaleYearPair2.getLocation())) == 0 ? 
				(locationMaxSaleYearPair1.getSale().compareTo(locationMaxSaleYearPair2.getSale())) : 
					cmp;
	}
}
