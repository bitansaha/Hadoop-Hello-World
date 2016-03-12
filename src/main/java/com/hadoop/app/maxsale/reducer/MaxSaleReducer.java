package com.hadoop.app.maxsale.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxSaleReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int maxSale = 0;
		for (IntWritable eachSale : values) {
			if (eachSale.get() > maxSale)
				maxSale = eachSale.get();
		}
		
		context.write(key, new IntWritable(maxSale));
	}
}
