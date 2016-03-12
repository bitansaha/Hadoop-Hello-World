package com.hadoop.app.totalsort.wordcount.v1;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.app.totalsort.wordcount.v1.mapper.WordCountPreparationMapper;
import com.hadoop.app.totalsort.wordcount.v1.reducer.WordCountReducer;

public class WordCountChain extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Word Count V1 (Chain)");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Configuration wcpMapperConf = new Configuration(false);
		ChainMapper.addMapper(job, WordCountPreparationMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, wcpMapperConf);
		
		Configuration wcMapperConf = new Configuration(false);
		wcMapperConf.setClass("mapreduce.job.partitioner.class", TotalOrderPartitioner.class, Partitioner.class);
		ChainMapper.addMapper(job, Mapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, wcMapperConf);
		
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		InputSampler.Sampler<Text, IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(0.1, 10000, 10);
		InputSampler.writePartitionFile(job, sampler);
		job.addCacheFile( new URI( TotalOrderPartitioner.getPartitionFile(getConf()) ) );
		
		job.setNumReduceTasks(10);
		job.setReducerClass(WordCountReducer.class);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountChain(), args);
		System.exit(exitCode);
	}
}
