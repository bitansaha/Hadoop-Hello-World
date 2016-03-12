package com.hadoop.app.jobchain.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NeglectSingleWordMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void map(Text key, IntWritable value,
			Mapper<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if (value.get() > 1)
			context.write(key, value);
	}

}
