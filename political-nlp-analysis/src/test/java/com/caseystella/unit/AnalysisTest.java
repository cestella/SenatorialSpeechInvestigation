package com.caseystella.unit;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import com.caseystella.AnalysisJob;
import com.caseystella.mr.MapKey;
import com.caseystella.util.NLPUtil;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;


public class AnalysisTest extends TestCase {
	 /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AnalysisTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AnalysisTest.class );
    }
    
    /**
     * Test just the map phase
     * @throws Exception
     */
    public void testMapper() throws Exception
    {
    	AnalysisJob.Mapper mapper = new AnalysisJob.Mapper()
    	{
    		@Override
    		protected Set<String> getStopwords() {
    			
    			return ImmutableSet.of("the", "a", "is", "in");
    		}
    	};
    	
    	//Mock out the OutputCollector with one which just updates my list of key/value pairs.
    	
    	final List<SimpleImmutableEntry<MapKey, Text>> mapOutput = new ArrayList<SimpleImmutableEntry<MapKey, Text>>();
    	mapper.map( new LongWritable(0)
    			  , new Text("100\tThe cat in the hat??||    is. a good booking...")
    			  , new OutputCollector<MapKey, Text>() 
    			  	{
    					@Override
    					public void collect(MapKey key, Text value)
    							throws IOException 
    					{
    						mapOutput.add(new SimpleImmutableEntry<MapKey, Text>(key, value));
    					}
					}
    			  , null
    			  );
    	//verify the output given the input.
    	assertEquals(mapOutput.get(0).getKey().getDocumentId(), 100);
    	assertEquals(mapOutput.get(0).getKey().getTerm(), "cat");
    	assertEquals(mapOutput.get(1).getKey().getDocumentId(), 100);
    	assertEquals(mapOutput.get(1).getKey().getTerm(), "hat");
    	assertEquals(mapOutput.get(3).getKey().getDocumentId(), 100);
    	assertEquals(mapOutput.get(3).getKey().getTerm(), "book");
    	assertEquals(mapOutput.get(3).getValue().toString(), "booking");
    	assertEquals(mapOutput.size(), 4);
    }
    
    private static ListMultimap<MapKey, Text> newMultimap()
    {
    	return Multimaps.newListMultimap( new TreeMap<MapKey, Collection<Text>>()
	   			   , new Supplier<ArrayList<Text>>()
	   			   	 {
						@Override
						public ArrayList<Text> get() {
							return new ArrayList<Text>();
						}
	   			   	 }
	   			   );
    }
    
    /**
     * Test the map and the reduce cycle completely
     * @throws Exception
     */
    public void testMapReduce() throws Exception
    {
    	String[] documents 
    		= new String[] { "1\tA man, a plan, a canal, panama!!!  He smells like a coal miner's cat."
    					   , "2\tMan has no conception of the evil in a cat's heart and cat brains. Reinvigorating like cats."
    					   , "3\tSmells like chicken fingers and conceptions. Reinvigorated."
    					   };
    	AnalysisJob.Mapper mapper = new AnalysisJob.Mapper()
							    	{
							    		@Override
							    		protected Set<String> getStopwords() {
							    			return  ImmutableSet.of("the", "a", "is", "in", "like");
							    		}
							    	};
		//this is a tree-backed multimap with a compare function derived from the map keys.
	    //this is doing the sort phase under the hood.
    	final ListMultimap<MapKey, Text> mapOutput = newMultimap();
					
    	{
    		final ListMultimap<MapKey, Text> intermediateOutput = newMultimap();
    		
	    	//same trick as in the testMapper() case
	    	OutputCollector<MapKey, Text> collector = 
	    			new OutputCollector<MapKey, Text>() 
    			  	{
    					@Override
    					public void collect(MapKey key, Text value)
    							throws IOException 
    					{
    						intermediateOutput.put(key, value);
    					}
					};
	    	
	    	for(int i = 0;i < documents.length;++i)
	    	{
	    		mapper.map( new LongWritable((long)i)
	    				  , new Text(documents[i])
	    				  , collector
	    				  , null
	    				  );
	    	}
    	
	    	//before it gets to the combiner, there were 3 unique tokens that map to cat
	    	assertEquals(3, intermediateOutput.get(new MapKey("cat", 2)).size());
    	
	    	AnalysisJob.Combiner combiner = new AnalysisJob.Combiner();
	    	for(Entry<MapKey, Collection<Text> > kvp : intermediateOutput.asMap().entrySet())
	    	{
	    		combiner.reduce( kvp.getKey()
	    					   , kvp.getValue().iterator()
	    					   ,new OutputCollector<MapKey, Text>() 
	    	    			  	{
	    	    					@Override
	    	    					public void collect(MapKey key, Text value)
	    	    							throws IOException 
	    	    					{
	    	    						mapOutput.put(key, value);
	    	    					}
	    	    			  	}
	    					   , null
	    					   );
	    	}
	    	//after the combiner, this gets reduced to 2 if the combiner does its job
	    	assertEquals(2, mapOutput.get(new MapKey("cat", 2)).size());
    	}
    	final Map<String, Object[]> reducerOutput = new HashMap<String, Object[]>();
    	
    	/*
    	 * Now inject the total number of documents so that the IDF is computable.
    	 */
    	AnalysisJob.Reducer reducer = new AnalysisJob.Reducer()
								    	{
								    		@Override
								    		protected int getTotalNumDocs() {
								    			return 3;
								    		}
								    	};
    	{
    		/*
    		 * Now just pass it through the reducer and collect it just like we did in the mapper test.
    		 * We want to key it on the lemmatized term and verify properties on individual terms.
    		 */
    		for(Entry<MapKey, Collection<Text> > kvp : mapOutput.asMap().entrySet())
	    	{
    			reducer.reduce( kvp.getKey()
    						  , kvp.getValue().iterator()
    						  , new OutputCollector<DoubleWritable, Text>()
    						  {
    								public void collect(DoubleWritable arg0, Text arg1) throws IOException 
    								{
    									double score = arg0.get();
    									Iterator<String> it = Splitter.on('\t')
									   			 					  .split(arg1.toString())
									   			 					  .iterator();
    									
    									String token = it.next();
    									reducerOutput.put( token
    													 , new Object[]
    													   {
    														 score
    													   , Lists.newArrayList(Splitter.on(' ')
    													   			 					.split(it.next())
    													   			 		    )
    													   }
    													 );
    								}
    						  }
    						  , null
    						  );
	    	}
    		
    		//verify everything went as expected.
    		assertEquals(NLPUtil.INSTANCE.IDF(3, 2), reducerOutput.get("cat")[0]);
    		assertEquals(Lists.newArrayList("cats", "cat"), (List<String>)reducerOutput.get("cat")[1]);
    		assertEquals(NLPUtil.INSTANCE.IDF(3, 1), reducerOutput.get("brain")[0]);
    		
    		//Easy, Peasy, Lemon Squeezy!
    	}
    	
    }
}
