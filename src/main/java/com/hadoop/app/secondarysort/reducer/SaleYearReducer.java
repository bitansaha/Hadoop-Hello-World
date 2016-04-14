package com.hadoop.app.secondarysort.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.app.secondarysort.key.LocationMaxSaleYearPair;
import com.hadoop.app.secondarysort.value.SaleYearPair;

public class SaleYearReducer extends Reducer<LocationMaxSaleYearPair, SaleYearPair, LocationMaxSaleYearPair, Text>{
	@Override
	protected void reduce(
			LocationMaxSaleYearPair locationMaxSaleYearPair,
			Iterable<SaleYearPair> saleYearPairs,
			Reducer<LocationMaxSaleYearPair, SaleYearPair, LocationMaxSaleYearPair, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		
		sb.append("\n");
		sb.append(" =============================================================================================================== ");
		sb.append("\n");

		int count = 0;
		for (Iterator<SaleYearPair> saleYearPairsIterator = saleYearPairs.iterator(); saleYearPairsIterator.hasNext(); ) {
			SaleYearPair eachSaleYearPair = saleYearPairsIterator.next();
			if (++count > 10) {
				sb.append("\n");
				count = 0;
			}
			sb.append(eachSaleYearPair.toString());
			sb.append(", ");
		}
		
		sb.append("\n");
		sb.append(" =============================================================================================================== ");
		sb.append("\n");
		sb.append("\n");
		
		context.write(locationMaxSaleYearPair, new Text(sb.toString()));
	}
}
