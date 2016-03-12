package com.hadoop.app.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.hadoop.app.maxsale.mapper.MaxSaleMapper;

public class MaxSaleMapperTest {

	@Test
	public void testMap() throws IOException {
		Text value = new Text("2007" + "\t" + "1109" + "\t" + "16");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxSaleMapper())
		.withInput(new LongWritable(0), value)
		.withOutput(new Text("2007"), new IntWritable(16))
		.runTest();
	}

}
