package com.caseystella.preprocessor;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class LuceneUtil {
	public static List<String> tokenize(Analyzer analyzer, String field, Reader input) {

        List<String> result = new ArrayList<String>();
        TokenStream stream  = analyzer.tokenStream(field, input);

        try {
            while(stream.incrementToken()) {
                result.add(stream.getAttribute(TermAttribute.class).term());
            }
        }
        catch(IOException e) {
            // not thrown b/c we're using a string reader...
        }

        return result;
    }  
}
