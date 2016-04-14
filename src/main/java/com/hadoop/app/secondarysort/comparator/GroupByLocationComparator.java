package com.hadoop.app.secondarysort.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;

public class GroupByLocationComparator extends WritableComparator{

	protected GroupByLocationComparator() {
		super(LocationMaxSaleYearPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable writableComparable1, WritableComparable writableComparable2) {
		LocationMaxSaleYearPair locationMaxSaleYearPair1 = (LocationMaxSaleYearPair)writableComparable1;
		LocationMaxSaleYearPair locationMaxSaleYearPair2 = (LocationMaxSaleYearPair)writableComparable2;
		
		Integer location1 = locationMaxSaleYearPair1.getLocation();
		Integer location2 = locationMaxSaleYearPair2.getLocation();
		
		return location1.compareTo(location2);
	}
}
