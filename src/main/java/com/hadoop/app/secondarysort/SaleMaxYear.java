package com.hadoop.app.secondarysort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.app.secondarysort.comparator.GroupByLocationComparator;
import com.hadoop.app.secondarysort.comparator.MaxSaleYearKeyComparator;
import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;
import com.hadoop.app.secondarysort.mapper.LocationSaleYearMapper;
import com.hadoop.app.secondarysort.partitioner.LocationPartitioner;
import com.hadoop.app.secondarysort.reducer.SaleYearReducer;
import com.hadoop.app.secondarysort.value.SaleYearPair;

public class SaleMaxYear extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Total_Sale_Max_Year");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LocationSaleYearMapper.class);
		job.setReducerClass(SaleYearReducer.class);
		
		
		// Partitioner (LocationPartitioner) makes sure that composite KEY's with a similar 'Location' goes to the same Reducer 
		// (by restricting the partitioning over a limited set of attributes)
		// By default it uses HashPartitioner which treats a composite KEY as a whole (considers all the attributes of the composite KEY)
		// for example {Location, Year, Sale} and hence two keys with the same Location might end-up with different Reducer due to different Year/Sale 
		job.setPartitionerClass(LocationPartitioner.class);
		
		// Grouping Comparator (GroupByLocationComparator) makes sure all KEY's (and their respective VALUE's) are grouped together in the Reduce phase
		// using some attribute from the composite KEY (in our case it is the Location attribute of the KEY)
		job.setGroupingComparatorClass(GroupByLocationComparator.class);
		
		// Sort Comparator (MaxSaleYearKeyComparator) Sort's the KEY's with in a SINGLE GROUP based on some attribute of the composite KEY (in our case it is the
		// Sale attribute of the KEY). The Reducer's output KEY is the TOP most of the sorted KEY's 
		job.setSortComparatorClass(MaxSaleYearKeyComparator.class);
		
		job.setMapOutputKeyClass(LocationMaxSaleYearPair.class);
		job.setMapOutputValueClass(SaleYearPair.class);
		job.setOutputKeyClass(LocationMaxSaleYearPair.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(5);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SaleMaxYear(), args);
		System.exit(exitCode);
	}
}
