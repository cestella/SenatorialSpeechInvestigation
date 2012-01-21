package com.caseystella.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class MapKey implements WritableComparable<MapKey> 
{
	private String term;
	private int documentId;
	public MapKey(String term, int documentId)
	{
		this.term = term;
		this.documentId = documentId;
	}
	public int getDocumentId() {
		return documentId;
	}
	public String getTerm() {
		return term;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException 
	{
		term = input.readUTF();
		documentId = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException 
	{
		output.writeUTF(term);
		output.writeInt(documentId);
	}

	@Override
	public int compareTo(MapKey other) {
		ComparisonChain chain = ComparisonChain.start();
		return chain.compare(term, other.term)
					.compare(documentId, other.documentId)
					.result();
	}

}
