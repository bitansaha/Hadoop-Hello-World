package com.hadoop.app.totalsort.wordcount.v1;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.app.totalsort.wordcount.v1.reducer.WordCountReducer;

public class WordCount extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Word Count V1");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setReducerClass(WordCountReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(10);
		
		// Partition class will look at the samples and create 'n' number of buckets and then look at the Mapper output KEY and put it 
		// in any of the bucket. By default it uses the HashPartitioner which creates the partition based on the number of Reducers. 
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		// InputFormatClass (RecordReader used within will define what the sampler will read out of the file)
		job.setInputFormatClass(SequenceFileInputFormat.class);
		InputSampler.Sampler<Text, IntWritable> sampler = new InputSampler.RandomSampler<Text, IntWritable>(0.1, 10000, 10);
		// This is a blocking call (probably the MR master creates the samples *buckets* in a single threaded form)
		InputSampler.writePartitionFile(job, sampler);
		
		job.addCacheFile( new URI( TotalOrderPartitioner.getPartitionFile(getConf()) ) );
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

}
