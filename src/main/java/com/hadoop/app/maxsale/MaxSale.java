package com.hadoop.app.maxsale;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.app.maxsale.mapper.MaxSaleMapper;
import com.hadoop.app.maxsale.reducer.MaxSaleReducer;

/**
 * Hello world!
 *
 */
public class MaxSale 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Job job = new Job();
        job.setJarByClass(MaxSale.class);
        job.setJobName("Max Sale");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(MaxSaleMapper.class);
        job.setReducerClass(MaxSaleReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit( (job.waitForCompletion(true) ? 0 : 1) );
    }
}
