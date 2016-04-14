package com.hadoop.app.secondarysort.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;
import com.hadoop.app.secondarysort.value.SaleYearPair;

public class LocationPartitioner extends Partitioner<LocationMaxSaleYearPair, SaleYearPair>{

	@Override
	public int getPartition(LocationMaxSaleYearPair key, SaleYearPair value,
			int numPartitions) {
		return Math.abs(key.getLocation() * 127) % numPartitions;
	}

}
