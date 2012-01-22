package com.caseystella.integration;

import java.io.File;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import com.caseystella.AnalysisJob;
import com.caseystella.preprocessor.DataPreprocessor;
import com.caseystella.preprocessor.DataPreprocessor.PoliticalOrientation;
import com.google.common.io.Files;

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
    	}
    }
}
