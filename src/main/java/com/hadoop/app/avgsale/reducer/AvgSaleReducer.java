package com.hadoop.app.avgsale.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgSaleReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int totalSales = 0;
		int salesCount = 0;
		
		for (IntWritable sale : values)
		{
			totalSales += sale.get();
			salesCount += 1;
		}
		
		int avgSale = totalSales / salesCount;
		
		context.write(key, new IntWritable(avgSale));
	}
}
