package com.caseystella.integration;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AnalysisIntegrationTest extends TestCase
{
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
    
    public void preprocessData()
    {
    	
    }
}
