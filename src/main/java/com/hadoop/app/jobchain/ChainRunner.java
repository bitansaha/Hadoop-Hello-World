package com.hadoop.app.jobchain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.app.jobchain.mapper.MarkTextMapper;
import com.hadoop.app.jobchain.mapper.NeglectSingleWordMapper;
import com.hadoop.app.jobchain.mapper.TextExtractMapper;
import com.hadoop.app.jobchain.reducer.WordCountReducer;

public class ChainRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Chain Runner");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Configuration textExtractConf = new Configuration(false);
		ChainMapper.addMapper(job, TextExtractMapper.class, LongWritable.class, Text.class, Text.class, NullWritable.class, textExtractConf);
		
		Configuration markTextConf = new Configuration(false);
		ChainMapper.addMapper(job, MarkTextMapper.class, Text.class, NullWritable.class, Text.class, IntWritable.class, markTextConf);
		
		Configuration wrdCountConf = new Configuration(false);
		ChainReducer.setReducer(job, WordCountReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, wrdCountConf);
		
		Configuration neglectSingleWordConf = new Configuration(false);
		ChainReducer.addMapper(job, NeglectSingleWordMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, neglectSingleWordConf);
		
		job.setNumReduceTasks(10);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ChainRunner(), args);
		System.exit(exitCode);
	}

}
