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
	public MapKey()
	{
		this(null, -1);
	}
	
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + documentId;
		result = prime * result + ((term == null) ? 0 : term.hashCode());
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
		MapKey other = (MapKey) obj;
		if (documentId != other.documentId)
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}
	
	

}
