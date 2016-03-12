package com.hadoop.app.reducer;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.hadoop.app.maxsale.reducer.MaxSaleReducer;

public class MaxSaleReducerTest {

	@Test
	public void testReduce() throws IOException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxSaleReducer())
		.withInput(new Text("2007"), Arrays.asList(new IntWritable(20), new IntWritable(82), new IntWritable(52)))
		.withOutput(new Text("2007"), new IntWritable(82))
		.runTest();
	}

}
