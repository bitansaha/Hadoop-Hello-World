package com.hadoop.app.totalsort.wordcount.v2.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> ite = values.iterator();
		long count = 0;
		while (ite.hasNext()) {
			count += ite.next().get();
		}
		context.write(key, new LongWritable(count));
	}
}
