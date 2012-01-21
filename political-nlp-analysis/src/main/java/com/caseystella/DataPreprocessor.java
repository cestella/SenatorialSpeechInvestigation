package com.caseystella;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

public class DataPreprocessor 
{
	private static enum PoliticalOrientation
	{
		CONSERVATIVE((byte)0x0)
	   ,MODERATE((byte)0x1)
	   ,LIBERAL((byte)0x2)
	   ;
	   
	   private byte code;
	   
	   private PoliticalOrientation(byte code)
	   {
		   this.code = code;
	   }
	   public byte getCode()
	   {
		   return code;
	   }
	}
	
	private static class DocumentDescriptor
	{
		public int documentId;
		public File documentLocation;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + documentId;
			result = prime
					* result
					+ ((documentLocation == null) ? 0 : documentLocation
							.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DocumentDescriptor other = (DocumentDescriptor) obj;
			if (documentId != other.documentId)
				return false;
			if (documentLocation == null) {
				if (other.documentLocation != null)
					return false;
			} else if (!documentLocation.equals(other.documentLocation))
				return false;
			return true;
		}
		
		
		
	}
	
	private static class Preprocessor extends Analyzer
	{
		Set<String> stopwords = new HashSet<String>();
		public Preprocessor(File stopwordsFile) throws IOException
		{
			for(String stopword : Files.readLines(stopwordsFile, Charsets.US_ASCII))
			{
				stopwords.add(stopword.toLowerCase());
			}
		}
		
		@Override
		public TokenStream tokenStream(String arg0, Reader reader) {
			
			return new PorterStemFilter(new StopFilter(true, new LowerCaseTokenizer(reader), stopwords) );
		}
	}
	
	
	private static BiMap<String, Double> getIdealPointsMap(File idealPointsFile) throws IOException
	{
		final BiMap<String, Double> map = HashBiMap.create();
		final Splitter lineSplitter = Splitter.on(',')
								 		  	  .trimResults();
		final Splitter nameSplitter = Splitter.on(" ")
				  							  ;
		return Files.readLines( idealPointsFile
							  , Charsets.US_ASCII
							  , new LineProcessor< BiMap<String, Double> >() 
							    {
									@Override
									public boolean processLine(String line)
											throws IOException 
									{
										List<String> tokens = Lists.newArrayList(lineSplitter.split(line));
										if(tokens.size() > 2 )
										{
											try
											{
												double mean = Double.parseDouble(tokens.get(1));
												String name = nameSplitter.split(tokens.get(0)).iterator().next().substring(1);
												map.put(name, mean);
											}
											catch(NumberFormatException nfe)
											{
												return true;
											}
										}
										return true;
									}
									@Override
									public BiMap<String, Double> getResult() {
										return map;
									}
								}
							  );
	}
	
	private static EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitionDataDirectory( File dataDirectory
																								, BiMap<String, Double> idealPointMap
																								, double conservativeRightBoundary
																								, double liberalLeftBoundary
																								)
	{
		EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitions 
			= new EnumMap<DataPreprocessor.PoliticalOrientation, List<DocumentDescriptor>>(PoliticalOrientation.class);
		for(PoliticalOrientation orientation : PoliticalOrientation.values())
		{
			partitions.put(orientation, new ArrayList<DocumentDescriptor>());
		}
		List<File> documents = Arrays.asList(dataDirectory.listFiles());
		
		int documentId = 0;
		Splitter nameSplitter = Splitter.on('_')
										.trimResults()
										;
		Splitter lastNameSplitter = Splitter.on('-')
											.trimResults()
											;
		for(File document : documents)
		{
			DocumentDescriptor descriptor = new DocumentDescriptor();
			String wholeName = nameSplitter.split(document.getName()).iterator().next();
			String lastName = Lists.newArrayList(lastNameSplitter.split(wholeName)).get(1).toUpperCase();
			descriptor.documentId = documentId++;
			descriptor.documentLocation = document;
			
			double idealPoint = idealPointMap.get(lastName);
			
			if(idealPoint < conservativeRightBoundary)
			{
				partitions.get(PoliticalOrientation.CONSERVATIVE).add(descriptor);
			}
			else if(idealPoint < liberalLeftBoundary)
			{
				partitions.get(PoliticalOrientation.MODERATE).add(descriptor);
			}
			else
			{
				partitions.get(PoliticalOrientation.LIBERAL).add(descriptor);
			}
		}
		
		
		return partitions;
	}
	
	
	private static void dumpDocument( PrintWriter pw
									, DocumentDescriptor descriptor
									, Analyzer preprocessor
									, PoliticalOrientation orientation
									) throws FileNotFoundException
	{
		List<String> tokens = LuceneUtil.tokenize(preprocessor, "", new BufferedReader(new FileReader(descriptor.documentLocation)));
		for(String token : tokens)
		{
			pw.println(Joiner.on(" ").join(orientation.getCode(), descriptor.documentId, token));
		}
	}
	private static void dumpData( EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitions
								, Analyzer preprocessor 
								, File outputDir
								) throws FileNotFoundException
	{
		for(PoliticalOrientation orientation : PoliticalOrientation.values())
		{
			PrintWriter pw = new PrintWriter(new File(outputDir, orientation.toString()));
			for(DocumentDescriptor descriptor : partitions.get(orientation))
			{
				dumpDocument(pw, descriptor, preprocessor, orientation);
			}
			pw.close();
		}
	}
	
	public static void main(String... argv) throws ParseException, IOException
	{
		CommandLineParser parser = new PosixParser();
		// create Options object
		Options options = new Options();
		
		{
			options.addOption( OptionBuilder.withArgName("input_dir")
											.hasArg()
											.withDescription("Data input directory")
											.isRequired()
											.withLongOpt("input-dir")
											.create("i")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("output_dir")
											.hasArg()
											.withDescription("Data output directory")
											.isRequired()
											.withLongOpt("output-dir")
											.create("o")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("conservative_right_boundary")
											.hasArg()
											.withDescription("The conservative right boundary")
											.isRequired()
											.withLongOpt("conservative-right-boundary")
											.create("c")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("liberal_left_boundary")
											.hasArg()
											.withDescription("The liberal left boundary")
											.isRequired()
											.withLongOpt("liberal-left-boundary")
											.create("l")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("ideal_points_file")
											.hasArg()
											.withDescription("The ideal points file")
											.isRequired()
											.withLongOpt("ideal-points-file")
											.create("p")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("stopword_file")
											.hasArg()
											.withDescription("The stopword file")
											.isRequired()
											.withLongOpt("stopword-file")
											.create("s")
							 );
		}
		
		// parse the command line arguments
	    CommandLine line = parser.parse( options, argv );
	    
	    File inputDir = new File(line.getOptionValue("i"));
	    File outputDir = new File(line.getOptionValue("o"));
	    File idealPointsFile = new File(line.getOptionValue("p"));
	    File stopwordFile = new File(line.getOptionValue("s"));
	    
	    double conservativeRightBoundary = Double.parseDouble(line.getOptionValue("c"));

	    double liberalLeftBoundary = Double.parseDouble(line.getOptionValue("l"));
	   
	    BiMap<String, Double> idealPointsMap = getIdealPointsMap(idealPointsFile);
	    EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitions
	    	= partitionDataDirectory(inputDir, idealPointsMap, conservativeRightBoundary, liberalLeftBoundary);
	    
	    dumpData(partitions, new Preprocessor(stopwordFile), outputDir);
	    
	}
}
