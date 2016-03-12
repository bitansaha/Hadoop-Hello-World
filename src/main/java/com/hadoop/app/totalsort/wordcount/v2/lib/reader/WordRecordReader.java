package com.hadoop.app.totalsort.wordcount.v2.lib.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WordRecordReader extends RecordReader<Text, IntWritable>{

	private RecordReader<LongWritable, Text> primaryReader;
	private Iterator<String> keys;
	private Text currKey;
	private IntWritable currValue;
	
	public WordRecordReader(RecordReader<LongWritable, Text> primaryReader) {
		this.primaryReader = primaryReader;
		this.keys = (new ArrayList<String>()).iterator();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		primaryReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (keys.hasNext()) {
			currKey = new Text(keys.next());
			currValue = new IntWritable(1);
			return true;
		}
		
		if (primaryReader.nextKeyValue()) {
			keys = Arrays.asList( primaryReader.getCurrentValue().toString().split(" ") ).iterator();
			return nextKeyValue();
		}
		
		currKey = null;
		currValue = null;
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		return currValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ( keys.hasNext() && (primaryReader.getProgress() == 0.0f) ) ? 0.0f : Math.max(1.0f, primaryReader.getProgress());
	}

	@Override
	public void close() throws IOException {
		primaryReader.close();
	}
	
}
