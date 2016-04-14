package com.hadoop.app.secondarysort.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;
import com.hadoop.app.secondarysort.value.SaleYearPair;

public class LocationSaleYearMapper extends Mapper<LongWritable, Text, LocationMaxSaleYearPair, SaleYearPair>{

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, LocationMaxSaleYearPair, SaleYearPair>.Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		int year = Integer.parseInt(values[0]);
		int location = Integer.parseInt(values[1]);
		int sale = Integer.parseInt(values[2]);
		context.write(new LocationMaxSaleYearPair(location, year, sale), new SaleYearPair(sale, year));
	}
}
