package edu.umass.cs.gnsclient.benchmarking;

import java.sql.SQLException;


import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

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
//			GuidEntry guidEntry = SelectCallBenchmarking.client.guidCreate
//					(SelectCallBenchmarking.account_guid, guidAlias);
			
			String accountAlias = SelectCallBenchmarking.ACCOUNT_ALIAS_PREFIX+guidAlias
					+SelectCallBenchmarking.ACCOUNT_ALIAS_SUFFIX;
			GuidEntry guidEntry = GuidUtils.lookupOrCreateAccountGuid(SelectCallBenchmarking.client, 
					accountAlias, "password", true);
			
			SelectCallBenchmarking.client.setLocation(guidEntry, longitude, latitude);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}