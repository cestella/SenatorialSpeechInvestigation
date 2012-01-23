package com.caseystella.util;

import java.util.Set;

import com.caseystella.util.NLPUtil.ImmutableToken;

public interface INLPUtil {

	/**
	 * This computes the IDF given the total number of documents in the corpus and the number of documents containing the given word.
	 * This uses the modification to base IDF used in the Okapi BM25 ranking function. (http://en.wikipedia.org/wiki/Okapi_BM25#IDF_Information_Theoretic_Interpretation)
	 * @param N The total number of documents in the corpus
	 * @param d The total number of documents containing a particular word
	 * @return
	 */
	public abstract double IDF(int N, int d);

	/**
	 * Creates a iterable stream of tokens from a document filtered by stopwords.
	 * 
	 * This uses a simple whitespace tokenizer along with - that strips non-alphabetical characters and filters words under 3 characters.
	 * @param document
	 * @param stopwords
	 * @return
	 */
	public abstract Iterable<ImmutableToken> tokenizeDocument(String document,
			Set<String> stopwords);

}