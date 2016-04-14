package com.hadoop.app.secondarysort.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class LocationMaxSaleYearPair implements WritableComparable<LocationMaxSaleYearPair>{

	private IntWritable location;
	private IntWritable year;
	private IntWritable sale;
	
	/**
	 * This is required as Hadoop will try to invoke this through Reflection
	 */
	public LocationMaxSaleYearPair() {
		this.set(new IntWritable(), new IntWritable(), new IntWritable());
	}
	
	public LocationMaxSaleYearPair(int location, int year, int sale) {
		this.set(new IntWritable(location),new IntWritable(year), new IntWritable(sale));
	}
	
	private void set(IntWritable location, IntWritable year, IntWritable sale) {
		this.location = location;
		this.year = year;
		this.sale = sale;
	}
	
	public Integer getLocation() {
		return location.get();
	}
	
	public Integer getYear() {
		return year.get();
	}
	
	public Integer getSale() {
		return sale.get();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		location.write(out);
		year.write(out);
		sale.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		location.readFields(in);
		year.readFields(in);
		sale.readFields(in);
	}

	@Override
	public int compareTo(LocationMaxSaleYearPair locationMaxYearPair) {
		int cmp = 0;
		return ( (cmp = this.location.compareTo(locationMaxYearPair.location)) == 0 ? this.year.compareTo(locationMaxYearPair.year) : cmp );
	}

	/**
	 * Hadoop uses the {@link #toString()} method to write the custom key content to file 
	 */
	@Override
	public String toString() {
		return "( " + this.location.get() + ", " + this.year.get() + ", " + this.sale.get() + " )";
	}
	
}
