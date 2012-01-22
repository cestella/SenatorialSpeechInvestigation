package com.caseystella.preprocessor;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

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
		CONSERVATIVE
	   ,MODERATE
	   ,LIBERAL
	   ;
	   
	 
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
	
//	private static class Preprocessor extends Analyzer
//	{
//		Set<String> stopwords = new HashSet<String>();
//		public Preprocessor(File stopwordsFile) throws IOException
//		{
//			for(String stopword : Files.readLines(stopwordsFile, Charsets.US_ASCII))
//			{
//				stopwords.add(stopword.toLowerCase());
//			}
//		}
//		
//		@Override
//		public TokenStream tokenStream(String arg0, Reader reader) {
//			
//			return new PorterStemFilter(new StopFilter(true, new LowerCaseTokenizer(reader), stopwords) );
//		}
//	}
	
	
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
																								, double conservativeLeftBoundary
																								, double liberalRightBoundary
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
			
			if(idealPoint < liberalRightBoundary)
			{
				partitions.get(PoliticalOrientation.LIBERAL).add(descriptor);
			}
			else if(idealPoint < conservativeLeftBoundary)
			{
				partitions.get(PoliticalOrientation.MODERATE).add(descriptor);
			}
			else
			{
				partitions.get(PoliticalOrientation.CONSERVATIVE).add(descriptor);
			}
		}
		
		
		return partitions;
	}
	
	
	private static void dumpDocument( final PrintWriter pw
									, final DocumentDescriptor descriptor
									) throws IOException
	{
		pw.println(Joiner.on("\t")
						 .join( descriptor.documentId
							  , Files.readLines( descriptor.documentLocation
									  		   , Charsets.US_ASCII
									  		   , new LineProcessor<String>()
									  		   {
								  					private StringBuffer buffer = new StringBuffer();
								  					
								  					@Override
								  					public String getResult() {
								  						return buffer.toString().trim();
								  					}
								  					@Override
								  					public boolean processLine(String line)
								  						throws IOException 
								  					{
								  						String normalizedString = line.replaceAll("\\s+", " ").trim();
								  						buffer.append(normalizedString + " ");
								  						return true;
								  					}
									  		   }
									  		   )
							  )
				  );

	}
	
	private static void dumpData( EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitions
								, File outputDir
								) throws IOException
	{
		for(PoliticalOrientation orientation : PoliticalOrientation.values())
		{
			PrintWriter pw = new PrintWriter(new File(outputDir, orientation.toString()));
			for(DocumentDescriptor descriptor : partitions.get(orientation))
			{
				dumpDocument(pw, descriptor);
			}
			pw.close();
		}
	}
	
	public static void execute( File inputDir
							  , File outputDir
							  , File idealPointsFile
							  , double conservativeLeftBoundary
							  , double liberalRightBoundary
							  ) throws IOException
	{
		 BiMap<String, Double> idealPointsMap = getIdealPointsMap(idealPointsFile);
		    EnumMap<PoliticalOrientation, List<DocumentDescriptor>> partitions
		    	= partitionDataDirectory(inputDir, idealPointsMap, liberalRightBoundary, conservativeLeftBoundary );
		    
		    dumpData(partitions, outputDir);
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
			options.addOption( OptionBuilder.withArgName("conservative_left_boundary")
											.hasArg()
											.withDescription("The conservative left boundary")
											.isRequired()
											.withLongOpt("conservative-left-boundary")
											.create("c")
							 );
		}
		{
			options.addOption( OptionBuilder.withArgName("liberal_right_boundary")
											.hasArg()
											.withDescription("The liberal right boundary")
											.isRequired()
											.withLongOpt("liberal-right-boundary")
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
		
		
		// parse the command line arguments
	    CommandLine line = parser.parse( options, argv );
	    
	    File inputDir = new File(line.getOptionValue("i"));
	    File outputDir = new File(line.getOptionValue("o"));
	    File idealPointsFile = new File(line.getOptionValue("p"));
	   
	    
	    double conservativeLeftBoundary = Double.parseDouble(line.getOptionValue("c"));

	    double liberalRightBoundary = Double.parseDouble(line.getOptionValue("l"));
	   
	   
	    execute(inputDir, outputDir, idealPointsFile, conservativeLeftBoundary, liberalRightBoundary);
	}
}
