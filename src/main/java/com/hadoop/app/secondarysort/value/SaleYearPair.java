package com.hadoop.app.secondarysort.value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class SaleYearPair implements WritableComparable<SaleYearPair>{

	private IntWritable sale;
	private IntWritable year;
	
	/**
	 * This is required as Hadoop will try to invoke this through Reflection
	 */
	public SaleYearPair() {
		this.set(new IntWritable(), new IntWritable());
	}
	
	public SaleYearPair(int sale, int year) {
		this.set(new IntWritable(sale), new IntWritable(year));
	}
	
	private void set(IntWritable sale, IntWritable year) {
		this.sale = sale;
		this.year = year;
	}
	
	public Integer getSale() {
		return this.sale.get();
	}
	
	public Integer getYear() {
		return this.year.get();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		sale.write(out);
		year.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sale.readFields(in);
		year.readFields(in);
	}

	@Override
	public int compareTo(SaleYearPair saleYearPair) {
		int cmp = 0;
		return (cmp = this.year.compareTo(saleYearPair.year)) == 0 ? (this.sale.compareTo(saleYearPair.sale)) : cmp  ;
	}
	
	/**
	 * Hadoop uses the {@link #toString()} method to write the custom value content to file 
	 */
	@Override
	public String toString() {
		return "( " + this.sale.get() + ", " + this.year.get() + " )";
	}

}
