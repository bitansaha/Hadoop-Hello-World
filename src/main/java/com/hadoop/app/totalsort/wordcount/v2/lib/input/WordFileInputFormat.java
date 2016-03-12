package com.hadoop.app.totalsort.wordcount.v2.lib.input;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.google.common.base.Charsets;
import com.hadoop.app.totalsort.wordcount.v2.lib.reader.WordRecordReader;

public class WordFileInputFormat extends FileInputFormat<Text, IntWritable>{

	@Override
	public RecordReader<Text, IntWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		String delimiter = context.getConfiguration().get("textinputformat.record.delimiter");
		byte[] recordDelimiterBytes = null;
		if (null != delimiter)
			recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		return new WordRecordReader(new LineRecordReader(recordDelimiterBytes));
	}
	
}
