package com.caseystella.util;

import java.util.Set;

import com.caseystella.util.NLPUtil.ImmutableToken;

public interface INLPUtil {

	public abstract double IDF(int N, int d);

	public abstract Iterable<ImmutableToken> tokenizeDocument(String document,
			Set<String> stopwords);

}