package com.caseystella.unit;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.caseystella.util.NLPUtil;
import com.caseystella.util.NLPUtil.ImmutableToken;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Unit test for the analysis job.
 */
public class NLPTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public NLPTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( NLPTest.class );
    }

    /**
     * Testing the whitespace tokenizer, stoplist, and the porter stemmer.
     */
    public void testTokenizer() throws Exception
    {
    	String sampleText;
    	{
    		sampleText = "The cat in the hat is a good book.";
    		ImmutableToken[] tokens = Iterables.toArray( NLPUtil.INSTANCE.tokenizeDocument( sampleText
					 																	  , ImmutableSet.of("the", "a", "is", "in")
    																					  )
    												   , ImmutableToken.class
    												   );
    		
    		assertEquals("cat", tokens[0].getToken());
    		assertEquals("cat", tokens[0].getStemmedToken());
    		assertEquals("hat", tokens[1].getToken());
    		assertEquals("hat", tokens[1].getStemmedToken());
    		assertEquals("good", tokens[2].getToken());
    		assertEquals("good", tokens[2].getStemmedToken());
    		assertEquals("book", tokens[3].getToken());
    		assertEquals("book", tokens[3].getStemmedToken());
    		assertEquals(4, tokens.length);
    	}
    	{
    		sampleText = "The cat in the hat??||    is. a good booking...";
    		ImmutableToken[] tokens = Iterables.toArray( NLPUtil.INSTANCE.tokenizeDocument( sampleText
					 																	  , ImmutableSet.of("the", "a", "is", "in")
    																					  )
    												   , ImmutableToken.class
    												   );
    		
    		assertEquals("cat", tokens[0].getToken());
    		assertEquals("cat", tokens[0].getStemmedToken());
    		assertEquals("hat", tokens[1].getToken());
    		assertEquals("hat", tokens[1].getStemmedToken());
    		assertEquals("good", tokens[2].getToken());
    		assertEquals("good", tokens[2].getStemmedToken());
    		assertEquals("booking", tokens[3].getToken());
    		assertEquals("book", tokens[3].getStemmedToken());
    		assertEquals(4, tokens.length);
    	}
    }
}
