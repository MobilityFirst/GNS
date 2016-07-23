package edu.umass.cs.gnsclient.benchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GuidEntry;

public class InsertTask implements Runnable
{
	private final String guidAlias;
	private final double latitude;
	private final double longitude;
	
	//private final JSONObject attrValJSON;
	private final AbstractRequestSendingClass requestSendingTask;
	
	public InsertTask( String guidAlias, double latitude, double longitude, 
			AbstractRequestSendingClass requestSendingTask )
	{
		this.guidAlias = guidAlias;
		this.latitude  = latitude;
		this.longitude = longitude;
		this.requestSendingTask = requestSendingTask;
	}
	
	@Override
	public void run()
	{
		try
		{
			long start = System.currentTimeMillis();
			insertGUIDRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guidAlias, end-start);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void insertGUIDRecord() throws SQLException
	{
		try 
		{
			GuidEntry guidEntry = SelectCallBenchmarking.client.guidCreate
					(SelectCallBenchmarking.account_guid, guidAlias);
			
			SelectCallBenchmarking.client.setLocation(guidEntry, longitude, latitude);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}