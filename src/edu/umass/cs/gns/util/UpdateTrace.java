package edu.umass.cs.gns.util;

public class UpdateTrace
{

  public static final int UPDATE = 1;
  public static final int ADD = 2;
  public static final int REMOVE = 3;

	public int type;
	public String name;
	public UpdateTrace(String name, int type)
	{
		this.name = name;
		this.type = type;
	}
	
	public String toString()
	{
		return name + ":" + type;
	}

	
}
