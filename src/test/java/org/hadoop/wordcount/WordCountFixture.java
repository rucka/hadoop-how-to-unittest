package org.hadoop.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountFixture {

	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;

	public WordCountFixture() {
	}

	@Before
	public void setup(){
		WordCountMapper mapper =  new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);

	}

	@Test
	public void testMapper() throws IOException{
		mapDriver.withInput(new LongWritable(1), new Text("paul arturo paul paul arturo carlitos"));
	    mapDriver.withOutput(new Text("paul"), new IntWritable(1));
	    mapDriver.withOutput(new Text("arturo"), new IntWritable(1));
	    
	    mapDriver.withOutput(new Text("paul"), new IntWritable(1));
	    mapDriver.withOutput(new Text("paul"), new IntWritable(1));
	    mapDriver.withOutput(new Text("arturo"), new IntWritable(1));
	    mapDriver.withOutput(new Text("carlitos"), new IntWritable(1));
	    mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException{

		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("arturo"),values);
		reduceDriver.withOutput(new Text("arturo"), new IntWritable(2));
		reduceDriver.runTest();
	}
	@Test
	public void testMapReduce() throws IOException{
		mapReduceDriver.withInput(new LongWritable(1), new Text("paul arturo paul paul arturo carlitos"));
		mapReduceDriver.addOutput(new Text("arturo"), new IntWritable(2));
		mapReduceDriver.addOutput(new Text("carlitos"), new IntWritable(1));
		mapReduceDriver.addOutput(new Text("paul"), new IntWritable(3));
		mapReduceDriver.runTest();
	}
}