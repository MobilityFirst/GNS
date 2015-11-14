/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.contextsocket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.gns.GNSCalls;

import org.apache.log4j.Logger;

public class ContextWriterInternals
{
	public static final int GET 										= 1;
	public static final int PUT 										= 2;
	public static final int GET_ALL 									= 3;
	public static final int REMOVE  									= 4;
	
	private final String writerName;
	private final String writerGUID;
	
	private final String groupQuery;
	private final int numAttr;
	
	//directly reading from GNS
	private String groupGUID;
	
	//private double Lat 												= 100;
	//private double Longi 												= 100;
	//private double CHECK_RADIUS 										= 75;
	
	// group is calculated and updated per sec
	private int GROUP_UPDATE_DELAY 										= 5000;
	
	private ConcurrentMap<String, ContextSocket> memberConnectionMap 	= null;
	//private Thread listeningThread = null;
	//private JSONArray groupMembers = null;
	
	//private ContextServiceCallsSingleton<Integer> contextServCallsSingleton;
	
	// waits on this lock to recv reply of the query.
	public final Object contextServiceQueryWaitLock						= new Object();
	
	public boolean csQueryLockFlag										= false;
	
	private final ExecutorService     connectionSetupPool;
	// for exp measurements
	//private long startTime 											= 0;
	private static Logger log = Logger.getLogger(ContextWriterInternals.class.getName());
	
	public ContextWriterInternals(String writerName, String groupQuery) throws Exception
	{
		// register writerName in GNS or get GUID locally.
		this.writerName = writerName;
		this.writerGUID = writerName;
		
		this.groupQuery = groupQuery;
		numAttr = getNumberOfAttributes();
		
		//groupMembers = new JSONArray();
		memberConnectionMap = new ConcurrentHashMap<String, ContextSocket>();
		
		//contextServCallsSingleton = new ContextServiceCalls<Integer>(0, this);
		connectionSetupPool = Executors.newCachedThreadPool();
		
		//msocketGroupWriterInternalsObj.setGroupName(groupName);
		JSONArray currGroupMem = lookupGroupGUIDAndRead();
		Map<String, Boolean> jsoMap = convertJSONArrayToMap(currGroupMem);
		updateMainMap(jsoMap);
		
		
		//createGroup();
		//startGroupMaintainThread();
		ContextServiceCallsSingleton.registerForTrigger(this.groupQuery, this);
		
		//long noStart = System.currentTimeMillis();
		//GNSCalls.updateNotificationSetOfAGroup
		//			(ContextServiceCallsSingleton.getSocketAddress(), this.groupQuery, this.groupGUID);
		//long noEnd = System.currentTimeMillis();
		//System.out.println("MSocketGroupWriterInternals "+(lend-lstart));
	}
	
	public ConcurrentMap<String, ContextSocket> getConnectionMap()
	{
		return memberConnectionMap;
	}
	
	private Map<String, Boolean> convertJSONArrayToMap(JSONArray currGroupMem)
	{
		Map<String, Boolean> jsonMap = new HashMap<String, Boolean>();
		
		for( int i=0;i<currGroupMem.length();i++ )
		{
			try 
			{
				String memberGUID = currGroupMem.getString(i);
				jsonMap.put(memberGUID, true);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		return jsonMap;
	}
	
	
	private void updateMainMap(Map<String, Boolean> jsonMap)
	{
		// iterator should be used with lock
		//http://tutorials.jenkov.com/java-util-concurrent/concurrentmap.html
		List<String> tobeRemoved = new LinkedList<String>();
		synchronized(memberConnectionMap)
		{
			Iterator<Entry<String, ContextSocket>> entries = memberConnectionMap.entrySet().iterator();
			while (entries.hasNext())
			{
			  Entry<String, ContextSocket> thisEntry = (Entry<String, ContextSocket>) entries.next();
			  String key = thisEntry.getKey();
			  // not contained in the current group members
			  if( !jsonMap.containsKey(key) )
			  {
				  tobeRemoved.add(key);
			  }
			}
		}
		
		// removal can be done without lock
		for(int i=0;i<tobeRemoved.size();i++)
		{
			String key = tobeRemoved.get(i);
			memberConnectionMap.remove(key);
			
			// FIXME: close the MSocket connection upon removal, 
			// needs to be done in executor service
		}
		
		// add new entries
		Iterator<Entry<String, Boolean>> entries = jsonMap.entrySet().iterator();
		while (entries.hasNext())
		{
		  Entry<String, Boolean> thisEntry = (Entry<String, Boolean>) entries.next();
		  String key = thisEntry.getKey();
		  // not contained, add new group member
		  if( !memberConnectionMap.containsKey(key) )
		  {
			  memberConnectionMap.put(key, null);
			  // do the connection setup in a executor service, key is GUID
			  connectionSetupPool.submit(new ConnectionSetupClass(key));
		  }
		}
	}
	
	/*public void printGroupMembers()
	{
		System.out.println(groupMembers);
	}*/
	
	public synchronized void createGroup()
	{
		log.trace("group creation started");
		if(ContextSocketConfig.USE_GNS)
		{
			//JSONArray groupMembers = getGroupMembersGUIDs();
			//groupMemberOperations(PUT, groupMembers);
			
			JSONArray currGroupMem = lookupGroupGUIDAndRead();
			Map<String, Boolean> jsoMap = convertJSONArrayToMap(currGroupMem);
			updateMainMap(jsoMap);
		}
		else
		{
			//JSONArray groupMembers = lookupGroupGUIDAndRead();
			//groupMemberOperations(PUT, groupMembers);
			
			JSONArray currGroupMem = lookupGroupGUIDAndRead();
			Map<String, Boolean> jsoMap = convertJSONArrayToMap(currGroupMem);
			updateMainMap(jsoMap);
		}
		
		//long end = System.currentTimeMillis();
		//System.out.println("group creation complete "+(end - startTime) );
	}
	
	public String getGroupQuery()
	{
		return groupQuery;
	}
	
	public void setGroupUpdateDelay(int delay) 
	{
		GROUP_UPDATE_DELAY = delay;
	}
	
	public int getGroupUpdateDelay()
	{
		return GROUP_UPDATE_DELAY;
	}
	
	public synchronized Object writerConnectionMapOperations(int typeOfOper, String aliasMember, ContextSocket toPut)
	{
		switch(typeOfOper)
		{
			case GET:
			{
				return memberConnectionMap.get(aliasMember);
			}
			
			case PUT:
			{
				memberConnectionMap.put(aliasMember, toPut);
				break;
			}
			
			case GET_ALL:
			{
				return memberConnectionMap.values();
			}
			
			case REMOVE:
			{
				return memberConnectionMap.remove(aliasMember);
			}
		}
		return null;
	}
	
	public void setGroupGUID(String grpGUID)
	{
		this.groupGUID = grpGUID;
	}
	
	private JSONArray lookupGroupGUIDAndRead()
	{
		long start = System.currentTimeMillis();
		JSONArray grpMembers = null;
		try
		{
			this.groupGUID = GNSCalls.getGroupGUID(this.groupQuery);
			grpMembers = GNSCalls.readGroupMembers(groupQuery, groupGUID);
			long end = System.currentTimeMillis();
			log.trace("MSOCKETWRITERINTERNAL from GNS time "+(end-start)+" attr "+numAttr);
			
		} catch(Exception ex)
		{
			log.trace("My groupQuery "+groupQuery+" group not in GNS, contact context service");
			grpMembers = new JSONArray();
			this.csQueryLockFlag = false;
			long qcsStart = System.currentTimeMillis();
			ContextServiceCallsSingleton.sendQueryToContextService(groupQuery, grpMembers, this);
			
			synchronized(this.contextServiceQueryWaitLock)
			{
				while( !this.csQueryLockFlag )
				{
					try
					{
						this.contextServiceQueryWaitLock.wait();
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
			
			/*try
			{
				this.groupGUID = GNSCalls.getGroupGUID(this.groupQuery);
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch(GnsException e)
			{
				e.printStackTrace();
			}*/
			
			long end = System.currentTimeMillis();
			log.trace("MSOCKETWRITERINTERNAL from CS querytime "+(end-start)+" numAttr "+numAttr+" cstime "+(end-qcsStart)+ "grpMembers "+grpMembers);
		}
		return grpMembers;
	}
	
	private int getNumberOfAttributes()
	{
		int numAttr = 0;
		String grpQuery = this.groupQuery;
		String[] parsed = grpQuery.split(" ");
		for(int i=0;i<parsed.length;i++)
		{
			if(parsed[i].startsWith("context"))
			{
				numAttr++;
			}
		}
		return numAttr;
	}
	
	public void close()
	{
		//contextServCalls.stopThis();
		//FIXME: need to close existing msockets.
	}
	
	public void writeAll(byte[] arrayToWrite, int offset, int length)
	{
		// so that we don't need to do iterator, which can be used only in 
		// one thread at once.
		Vector<String> memberKeysVector = new Vector<String>();
		memberKeysVector.addAll(memberConnectionMap.keySet());
		
		
		for(int i=0;i<memberKeysVector.size(); i++)
		{
			String GUIDMember = memberKeysVector.get(i);
			
			log.trace("writing to "+GUIDMember);
			connectionSetupPool.execute(new writeTask(GUIDMember, arrayToWrite, offset, length));
		}
	}
	
	private class ConnectionSetupClass implements Runnable
	{
		private final String guidString;
		
		public ConnectionSetupClass(String guidString)
		{
			this.guidString = guidString;
		}
		
		@Override
		public void run()
		{	
			MSocket retSocket = null;
			try
			{
				String memberAlias = GNSCalls.getAlias(guidString);
				log.trace("creating new MSocket to "+ guidString + " alias "+ memberAlias);
				retSocket = new MSocket(memberAlias, 0);
			} catch(Exception ex)
			{
				System.out.println("IO Exception recieved on MSocket write, socket is really in trouble, closing the current MSocket and opening a new one");
			}
			
			if(retSocket != null)
			{
				ContextSocket contextSock = new ContextSocket(retSocket);
				memberConnectionMap.put(guidString, contextSock);
			}
		}
	}
	
	
	private class writeTask implements Runnable 
	{
		private String GUIDMember = "";
		private byte[] mesgArray = null;
		private int offset =0;
		private int length =0;
		
		public writeTask( String GUIDMember, byte[] mesgArray, int offset, int length) 
		{
			this.GUIDMember  = GUIDMember;
			
			this.mesgArray = mesgArray;
			this.offset = offset;
			this.length = length;
		}

		@Override
		public void run() 
		{
			ContextSocket retSocketInfo = memberConnectionMap.get(GUIDMember);
			
			if(retSocketInfo == null)
			{
				log.trace("returned socket info null");
			}
			else
			{
				try 
				{
					retSocketInfo.getOutputStream().write(mesgArray, offset, length);
				} catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	/*private JSONArray getGroupMembersGUIDs()
	{
		JSONArray grpMembers = null;
		try
		{
			long gstart = System.currentTimeMillis();
			grpMembers = GNSCalls.readGroupMembers(this.groupQuery, this.groupGUID);
			long gend = System.currentTimeMillis();
			log.trace("getGroupMembersGUIDs time "+(gend-gstart));
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return grpMembers;
	 }*/
	
	/*public synchronized JSONArray groupMemberOperations(int typeOfOper, JSONArray toPut)
	{
		switch(typeOfOper)
		{
			case GET:
			{
				//Vector<String> toReturn = new Vector<String>();
				//toReturn.addAll(groupMembers);
				return groupMembers;
				//break;
			}
			case PUT:
			{
				groupMembers = toPut;
				break;
			}
		}
		return null;
	}*/
	
	/*public synchronized void startGroupMaintainThread()
	{
		log.trace("starting group maintain thread");
		//GroupMemberMaintain grpObj = new GroupMemberMaintain(this);
		//new Thread(grpObj).start();
	}*/
	
	/*private class GroupMemberMaintain implements Runnable 
	{
		private MSocketGroupWriterInternals msocketGroupWriterInternalsObj = null;
		
		public GroupMemberMaintain(MSocketGroupWriterInternals msocketGroupWriterInternalsObj) 
		{
			this.msocketGroupWriterInternalsObj = msocketGroupWriterInternalsObj;	
		}
		
		@Override
		public void run() 
		{
			while(true)
			{
				msocketGroupWriterInternalsObj.createGroup();
				try 
				{
					Thread.sleep(GROUP_UPDATE_DELAY);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}*/
	
	/*private void regsiterGroupInGNS(String groupName) throws Exception 
	 * {
	 * GnsIntegration.registerGroupInGNS(groupName, null);
	}*/
	
	/*private JSONArray collectNearbyGuids() 
	 {
	    JSONArray guids = new JSONArray();
	    JSONArray coordJson = new JSONArray(Arrays.asList(getLongi(), getLat() ));
	    try 
	    {
		    JSONArray queryResult = GnsIntegration.selectNear(coordJson, getRadius(), null);
		    for (int i = 0; i < queryResult.length(); i++) {
		        JSONObject record = queryResult.getJSONObject(i);
		        String guidString = record.getString("GUID");
		        guids.put(guidString);
		    }
	    }
	    catch (Exception e) 
	    {
	       e.printStackTrace();
	    }
	    return guids;
	}

	private Vector<String> groupMembershipCheck(JSONArray memberGuids) 
	{
		int i=0;
		String selectQuery = getGroupName();
		String[] parsed =  selectQuery.split(":");
		String field = parsed[0];
		String value = parsed[1];
		Vector<String> currMem = new Vector<String>();
		
		for(i=0;i<memberGuids.length();i++) 
		{
			try 
			{
				String memberGUID = (String) memberGuids.get(i);
				
				//String alias = GnsIntegration.getAliasOfGUID(memberGUID, null);
				//log.trace("getting key value of "+alias);
				//log.trace("gp 1"+writerInternalObj.getGroupName()+" gpGNS "+GnsIntegration.getGroupNameFromGNS((String) memberGuids.get(i), null));
				if( GnsIntegration.getKeyValue( memberGUID, field, null).equals( value ) )
				{
					currMem.add(memberGUID);
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			} catch (Exception e) 
			{
				log.trace("excp in groupMembershipCheck");
				//e.printStackTrace();
			}
		}
		return currMem;
	}*/
	
	/*public synchronized void setLocation(double Lat, double Longi)
	{
		this.Lat = Lat;
		this.Longi = Longi;
		
		//this.groupName = groupName;
	}
	
	public double getLat() 
	{
		return this.Lat ;
	}
	
	public double getLongi() 
	{
		return this.Longi ;
	}
	
	public void setRadius(double Radius) 
	{
		CHECK_RADIUS = Radius;
		//msocketGroupWriterInternalsObj.setLocation(Lat, Longi);
	}
	
	public double getRadius() 
	{
		return CHECK_RADIUS;
	}
	
	public void setStartTime(long start) 
	{
		this.startTime = start;
	}
	
	public long getStartTime() 
	{
		return this.startTime;
	}*/
}