package com.hadoop.app.jobchain.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		NullWritable nullWritable = NullWritable.get();
		List<String> words = Arrays.asList(line.split(" "));
		for (String eachWord : words) {
			context.write(new Text(eachWord), nullWritable);
		}
	}
}
