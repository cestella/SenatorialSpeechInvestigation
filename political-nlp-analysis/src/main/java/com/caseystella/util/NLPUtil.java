package com.caseystella.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public enum NLPUtil 
{
	INSTANCE
	;
	
	public static class ImmutableToken
	{
		private String token;
		private String stemmedToken;
		public ImmutableToken(String token, String stemmedToken)
		{
			this.token = token;
			this.stemmedToken = stemmedToken;
		}
		
		public String getStemmedToken() {
			return stemmedToken;
		}
		
		public String getToken() {
			return token;
		}
	}
	private static final Pattern whitespacePattern = Pattern.compile("[ \t\r\n\\-]");
	
	public Iterable<ImmutableToken> tokenizeDocument( String document
													, Set<String> stopwords
													)
	{
		PorterStemmer stemmer = new PorterStemmer();
		int index = 0;
		Matcher whitespaceMatcher = null;
		List<ImmutableToken> tokens = new ArrayList<ImmutableToken>();
		while(document !=null && index < document.length())
        {
            whitespaceMatcher = whitespacePattern.matcher(document.substring(index));
            boolean found = whitespaceMatcher.find();
            String token = null;
            if(found)
            {
                int newIndex = index + whitespaceMatcher.end();
                //If length is zero, it means that the "token" was just a run of whitespace, so discard
                //Otherwise, add this as a new token
                if(document.substring(index, newIndex-1).length()>0)
                {
                	token = document.substring(index, newIndex - 1);
                }
                index = newIndex;
            }
            else
            {
                //This should never execute, because if index < line.length, then another token will be found,
                //even if it's just whitespace to be discarded
            	token = document.substring(index);
                index = document.length();
            }
            
            if(token != null && token.length() > 0)
            {
            	String tokenNormalized = token.toLowerCase().replaceAll("[^A-Za-z ]", "");
            	if(tokenNormalized.length() > 0 && !stopwords.contains(tokenNormalized))
            	{
            		tokens.add(new ImmutableToken(tokenNormalized, stemmer.stem(tokenNormalized)));
            	}
            }
            
        }
		
		return tokens;
	}
}
