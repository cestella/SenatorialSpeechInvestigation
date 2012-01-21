package com.caseystella;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.caseystella.mr.MapKey;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class IDF 
{
	public static String PARAM_TOTAL_NUM_DOCS = "totalNumDocs";
	
	public static class Mapper extends MapReduceBase 
							  implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, MapKey, NullWritable>
	{
		private static Splitter splitter = Splitter.on(" ");
		
		
		
		@Override
		public void map( LongWritable arg0
					   , Text line
					   , OutputCollector<MapKey, NullWritable> collector
					   , Reporter reporter
					   )
		throws IOException 
		{
			List<String> tokens = Lists.newArrayList(splitter.split(line.toString()));
			if(tokens.get(1).contains("?"))
			{
				return;
			}
			else
			{
				collector.collect(new MapKey(tokens.get(1), Integer.parseInt(tokens.get(0))), NullWritable.get());
			}
		}
	}
	
	public static class Combiner extends MapReduceBase
								 implements org.apache.hadoop.mapred.Reducer<MapKey, NullWritable, MapKey, NullWritable>
	{

		@Override
		public void reduce(MapKey key, Iterator<NullWritable> values,
				OutputCollector<MapKey, NullWritable> collector, Reporter reporter)
				throws IOException 
		{
			collector.collect(key, NullWritable.get());
		}
		
	}
	
	public static class Partitioner extends MapReduceBase implements org.apache.hadoop.mapred.Partitioner<MapKey, NullWritable>
	{

		@Override
		public int getPartition(MapKey key, NullWritable value, int numReducers) 
		{
			return key.getTerm().hashCode() % numReducers;
		}
		
	}
	
	public static class Reducer extends MapReduceBase
								implements org.apache.hadoop.mapred.Reducer<MapKey, NullWritable, Text, DoubleWritable>
	{

		String currentTerm = null;
		int numDocsForTerm = 0;
		int totalNumDocs = 0;
		
		@Override
		public void configure(JobConf job) {
			totalNumDocs = job.getInt(PARAM_TOTAL_NUM_DOCS, -1);
		}
		
		@Override
		public void reduce( MapKey key
						  , Iterator<NullWritable> values
						  , OutputCollector<Text, DoubleWritable> collector
						  , Reporter reporter
						  )
				throws IOException 
		{
			if(currentTerm != null && key.getTerm().equals(currentTerm))
			{
				numDocsForTerm++; 
			}
			else
			{
				if(currentTerm != null)
				{
					collector.collect(new Text(key.getTerm()), new DoubleWritable(1.0*numDocsForTerm/totalNumDocs));
				}
				currentTerm = key.getTerm();
				numDocsForTerm = 1;
			}
		}
	}
	
	public static void main(String... argv) throws IOException
	{
		String inFile = argv[0];
		int totalDocuments = Integer.parseInt(argv[1]);
		String outFile = argv[2];
		JobConf conf = new JobConf(IDF.class);
		conf.setJobName("idf - " + inFile + " -> " + outFile);
		conf.setInt(PARAM_TOTAL_NUM_DOCS, totalDocuments);
		
		// the keys are words (strings)
	    conf.setOutputKeyClass(MapKey.class);
	    // the values are counts (ints)
	    conf.setOutputValueClass(NullWritable.class);
	    
	    conf.setMapperClass(Mapper.class);
	    conf.setPartitionerClass(Partitioner.class);
	    conf.setCombinerClass(Combiner.class);
	    conf.setReducerClass(Reducer.class);
	    FileInputFormat.addInputPath(conf, new Path(inFile));
	    FileOutputFormat.setOutputPath(conf, new Path(outFile));

	    JobClient.runJob(conf);
	}
}
