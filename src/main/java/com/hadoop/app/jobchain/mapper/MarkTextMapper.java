package com.hadoop.app.jobchain.mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarkTextMapper extends Mapper<Text, NullWritable, Text, IntWritable>{

	@Override
	protected void map(Text key, NullWritable value,
			Mapper<Text, NullWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String keyStr = key.toString();
		IntWritable one = new IntWritable(1);
		
		Pattern regex = Pattern.compile("[$&+,:;=?@#|]");
		Matcher matcher = regex.matcher(keyStr);
		
		if (!matcher.find())
			context.write(new Text(keyStr.toUpperCase()), one);
	}
}
