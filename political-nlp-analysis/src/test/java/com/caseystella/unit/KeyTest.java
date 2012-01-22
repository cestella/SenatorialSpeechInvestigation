package com.caseystella.unit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.caseystella.mr.MapKey;

public class KeyTest extends TestCase 
{
	 /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public KeyTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( KeyTest.class );
    }
    
    public void testSort() throws Exception
    {
    	MapKey key1 = new MapKey("d", 1);
    	MapKey key2 = new MapKey("d", 2);
    	MapKey key3 = new MapKey("e", 2);
    	MapKey key4 = new MapKey("f", 1);
    	MapKey key5 = new MapKey("f", 5);
    	List<MapKey> keys = Arrays.asList( key3
    									 , key2
    									 , key5
    									 , key1
    									 , key4
    									 );
    	Collections.sort(keys);
    	assertEquals(key1, keys.get(0));
    	assertEquals(key2, keys.get(1));
    	assertEquals(key3, keys.get(2));
    	assertEquals(key4, keys.get(3));
    	assertEquals(key5, keys.get(4));
    }
    
    public void testEquals() throws Exception
    {
    	String[] terms = new String[] { "term", "term2", "" };
    	int[] documentIds = new int[] { -1, 1, 2 };
    	for(int i = 0;i < terms.length;++i)
    	{
    		for(int j = 0;j < terms.length;++j)
    		{
    			MapKey key1 = new MapKey(terms[i], documentIds[i]);
    			MapKey key2 = new MapKey(terms[j], documentIds[j]);
    			assertEquals(key1, key1);
    			
    			//equality should be reflexive and symmetric
    			assertTrue(key1.compareTo(key1) == 0);
    			assertTrue(key1.compareTo(key2) == -1*key2.compareTo(key1));
    			
    			if(i == j)
    			{
    				//they're the same
    				assertEquals(key1, key2);
    				assertEquals(key2, key1);
    				assertTrue(key1.compareTo(key2) == 0);
    				assertTrue(key2.compareTo(key1) == 0);
    			}
    			else
    			{
    				// should be different
    				assertTrue(!key1.equals(key2));
    				assertTrue(key1.compareTo(key2) != 0);
    				
    			}
    		}
    	}
    	
    }
    
    public void testPersistence() throws Exception
    {
    	MapKey key = null;
    	{
    		key = new MapKey("term", 1);
    		ByteArrayOutputStream bos = new ByteArrayOutputStream();
    		DataOutput outputBuffer = new DataOutputStream(bos);
    		key.write(outputBuffer);
    		
    		MapKey key2 = new MapKey();
    		key2.readFields(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
    		assertEquals(key, key2);
    		assertEquals(key.compareTo(key2) ,0);
    	}
    }

}
