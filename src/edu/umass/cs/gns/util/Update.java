package edu.umass.cs.gns.util;

public class Update
{
	public int seqNumber;
	public String name;
	public Update(String name, int seqNumber)
	{
		this.name = name;
		this.seqNumber = seqNumber;
	}
	
	public String toString()
	{
		return name + ":" + seqNumber;
	}

	
}
