package com.caseystella.integration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import com.caseystella.AnalysisJob;
import com.caseystella.preprocessor.DataPreprocessor;
import com.caseystella.preprocessor.DataPreprocessor.PoliticalOrientation;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

public class AnalysisIntegrationTest extends TestCase
{
	public static double LIBERAL_RIGHT_BOUNDARY = 0.61;
	public static double CONSERVATIVE_LEFT_BOUNDARY = -0.65;
	public static int DOCUMENT_TOTAL = 1408;
	
	 /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AnalysisIntegrationTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AnalysisIntegrationTest.class );
    }
    
    public void preprocessData() throws IOException
    {
    	
    	DataPreprocessor.execute( new File("src/test/data/senate_speeches/")
    							, new File("target/processed")
    							, new File("src/test/data/ideal_points.csv")
    							, LIBERAL_RIGHT_BOUNDARY
    							, CONSERVATIVE_LEFT_BOUNDARY
    							);
    }
    
    public void outputReport() throws IOException
    {
    	ListMultimap<String, String[]> commonMap = 
    	Multimaps.newListMultimap( new TreeMap<String, Collection<String[]>>()
	   			   , new Supplier<ArrayList<String[]>>()
	   			   	 {
						@Override
						public ArrayList<String[]> get() {
							return new ArrayList<String[]>();
						}
	   			   	 }
	   			   );
    	PrintWriter reportOut = new PrintWriter(new File("target/output/report"));
    	
    	for(PoliticalOrientation orientation : PoliticalOrientation.values())
    	{
    		File outputFile = new File("target/output/" + orientation);
    		PrintWriter out = new PrintWriter(new File("target/output/" + orientation + "/report"));
    		
    		File[] parts = outputFile.listFiles(new FilenameFilter()
    						{
    							@Override
    							public boolean accept(File arg0, String arg1) {
    								if(arg1.startsWith("part-"))
    								{
    									return true;
    								}
    								return false;
    							}
    						}
    					   );
    		final List<String> output = new ArrayList<String>();
    		
    		Comparator<String> comparator = new Comparator<String>()
    				{
    					@Override
    					public int compare(String o1, String o2) 
    					{
    						ArrayList<String> firstSplit = Lists.newArrayList(Splitter.on('\t')
    															  .split(o1));
    						ArrayList<String> secondSplit = Lists.newArrayList(Splitter.on('\t')
    															   .split(o2));
    						
    						return ComparisonChain.start()
    											  .compare(Double.parseDouble(firstSplit.get(0)), Double.parseDouble(secondSplit.get(0)))
    											  .compare(firstSplit.get(1), secondSplit.get(1))
    											  .compare(firstSplit.get(2), secondSplit.get(2))
    											  .result();
    											  
    					}
    				};
    		for(File part : parts)
    		{
    			Files.readLines( part
    						   , Charsets.US_ASCII
    						   , new LineProcessor<Void>() 
    						    {
    								@Override
    								public Void getResult() {
    									// TODO Auto-generated method stub
    									return null;
    								}
    								@Override
    								public boolean processLine(String line)
    										throws IOException 
    								{
    									output.add(line);
    									return true;
    								}
								}
    						   );
    		}
    		Collections.sort(output, comparator);
    		int cnt = 0;
    		for(String line : output)
    		{
    			if(cnt++ < 200)
    			{
    				ArrayList<String> split = Lists.newArrayList(Splitter.on('\t')
							  .split(line));
    				commonMap.put(split.get(1), new String[] {orientation.toString() , line});
    			}
    			out.println(line);
    		}
    		out.close();
    	}
    	for(Entry<String, Collection<String[]>> entry : commonMap.asMap().entrySet())
    	{
    		if(entry.getValue().size() == 1)
    		{
    			String[] value = entry.getValue().iterator().next();
    			reportOut.println(value[0] + "\t" + value[1]);
    		}
    	}
    	reportOut.close();
    }
    
    public void testAnalysisJob() throws IOException, InterruptedException
    {
    	preprocessData();
    	for(PoliticalOrientation orientation : PoliticalOrientation.values())
    	{
    		File outFile = new File("target/output/" + orientation);
    		
    		if(outFile.exists())
    		{
    			System.out.println("Removing stale data: " + outFile);
    			Files.deleteRecursively(outFile);
    		}
	    	JobConf conf = AnalysisJob.createJobConf( "target/processed/" + orientation
	    											, DOCUMENT_TOTAL
	    											, "target/output/" + orientation
	    											, new File("src/test/data/stopwords.txt")
	    											);
	    	//noone needs 100M of sort buffer for an integration test.
	    	conf.set("io.sort.mb", "50");
	    	RunningJob job = JobClient.runJob(conf);
	    	while(!job.isComplete())
	    	{
	    		Thread.sleep(5000);
	    	}
	    	assertTrue(job.isSuccessful());
    	}
    	outputReport();
    }
}
