package com.hadoop.app.avgsale;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.app.avgsale.mapper.AvgSaleMapper;
import com.hadoop.app.avgsale.reducer.AvgSaleReducer;



public class AvgSale extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
	
		Job job = new Job(getConf(), "Avg Sale");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AvgSaleMapper.class);
		job.setReducerClass(AvgSaleReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AvgSale(), args);
		System.exit(exitCode);
	}

}
