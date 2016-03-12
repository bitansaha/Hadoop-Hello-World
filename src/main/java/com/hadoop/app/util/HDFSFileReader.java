package com.hadoop.app.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileReader {

	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://c6401.ambari.apache.org:8020"), configuration);
		Path path = new Path("/user/vagrant/output/part-r-00000");
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			line = br.readLine();
		}
	}

}
