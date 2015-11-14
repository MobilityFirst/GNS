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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;


public class ContextServiceCallsSingleton<NodeIDType> implements PacketDemultiplexer<JSONObject>
{
	public static final int QUERY_MSG_FROM_USER_REPLY						= 9;
		
	// trigger mesg type
	//public static final int QUERY_MSG_FROM_USER 							= 2;
	//private enum Keys {QUERY};
	
	private final NodeIDType myID;
	private final CSNodeConfig<NodeIDType> csNodeConfig;
	
	private final JSONNIOTransport<NodeIDType> niot;
	private final JSONMessenger<NodeIDType> messenger;
	
	private final String sourceIP;
	public final int LISTEN_PORT;
	
	
	//private MSocketGroupWriterInternals grpWriterInt;
	
	// stores outstanding query req to context service.
	// JSON Array is the array in which the result is returned.
	private ConcurrentHashMap<Long, RequestStorageClass> outstandingReq 	= null;
	
	
	// keeps track of registered write internal socket for refresh triggers.
	private ConcurrentHashMap<String, List<ContextWriterInternals>> triggerRegistrations 			
																			= null;
	
	// this file should be in included in msocket.jar
	private String configFileName											= "contextServiceNodeSetup.txt";
	
	private LinkedList<InetSocketAddress> nodeList							= null;
	
	private Random rand														= null;
	
	private long userReqCount												= 0;
	
	private final Object userReqCountLock									= new Object();
	
	private final Object registerForTriggerLock								= new Object();
	
	private static ContextServiceCallsSingleton<?> thisClass				= null;
	
	private static Object createSingletonLock								= new Object();
	
	protected static Logger     log                       					= Logger.getLogger(ContextServiceCallsSingleton.class.getName());
	
	//public ContextServiceCallsSingleton(NodeIDType id, MSocketGroupWriterInternals grpWriterInt) throws IOException
	public ContextServiceCallsSingleton(NodeIDType id) throws IOException
	{
		nodeList = new LinkedList<InetSocketAddress>();	
		//System.out.println("readNodeInfo nodeList "+nodeList.size());
		
		readNodeInfo();
		
		//System.out.println("nodeList "+nodeList.size());
		
		rand = new Random(System.currentTimeMillis());
		
		// around maximum ports allowed
		LISTEN_PORT = 3000+rand.nextInt(50000);
		
		myID = id;
		
		csNodeConfig = new CSNodeConfig<NodeIDType>();
		
		outstandingReq = new ConcurrentHashMap<Long, RequestStorageClass>();
		triggerRegistrations = new ConcurrentHashMap<String, List<ContextWriterInternals>>();
		
		
		//this.grpWriterInt = grpWriterInt;
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, LISTEN_PORT));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//ContextServicePacketDemultiplexer pd;
		
		niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
		
		messenger = new JSONMessenger<NodeIDType>(niot);
		
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		pd.register(ContextServicePacket.PacketType.REFRESH_TRIGGER, this);
		messenger.addPacketDemultiplexer(pd);
		
		if(ContextSocketConfig.PERIODIC_GROUP_UPDATE)
		{
			GroupUpdateThread gpt = new GroupUpdateThread();
			new Thread(gpt).start();
		}
	}
	
	public static void stopThis()
	{
		if( thisClass != null )
		{
			thisClass.messenger.stop();
			thisClass.niot.stop();
		}
	}
	
	public static void sendQueryToContextService( String query, JSONArray grpMembers, 
			ContextWriterInternals grpWriterInt )
	{
		//System.out.println("sendQueryToContextService called");
		try
		{
			//System.out.println("sendQueryToContextService called");
			createSingleton();
			
			log.trace("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
					+ thisClass.userReqCount);
			
			RequestStorageClass reqStorClass = new RequestStorageClass(grpWriterInt, grpMembers);
			
			synchronized(thisClass.userReqCountLock)
			{
				thisClass.outstandingReq.put(thisClass.userReqCount, reqStorClass);
				thisClass.sendQueryMesgToContextService(query, thisClass.userReqCount);
				thisClass.userReqCount++;
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void sendUpdateToContextService( String memberGUID, String attrName, double newValue, 
			ContextMemberInternals grpmemberInt )
	{
		try
		{
			createSingleton();
			
			log.trace("CONTEXTSERVICE EXPERIMENT: UPDATEFROMUSER REQUEST ID "
					+ thisClass.userReqCount);
			
			//RequestStorageClass reqStorClass = new RequestStorageClass(grpWriterInt, grpMembers);
			
			synchronized(thisClass.userReqCountLock)
			{
				//thisClass.outstandingReq.put(thisClass.userReqCount, reqStorClass);
				thisClass.sendUpdateMesgToContextService(memberGUID, thisClass.userReqCount, attrName, newValue);
				thisClass.userReqCount++;
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void registerForTrigger(String query, ContextWriterInternals writerInternal)
	{
		try
		{
			createSingleton();
			
			log.trace("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
					+ thisClass.userReqCount +" AT "+System.currentTimeMillis()
					+" "+"contextATT0"+" QueryStart "+System.currentTimeMillis());
			
			List<ContextWriterInternals> notifierList = thisClass.triggerRegistrations.get(query);
			
			synchronized(thisClass.registerForTriggerLock)
			{
				if( notifierList == null )
				{
					notifierList = new LinkedList<ContextWriterInternals>();
					notifierList.add(writerInternal);
					thisClass.triggerRegistrations.put(query, notifierList);
				} else
				{
					notifierList.add(writerInternal);
				}
			}
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public static InetSocketAddress getSocketAddress()
	{
		try
		{
			createSingleton();
			return thisClass.getSocketAddressInternal();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	private InetSocketAddress getSocketAddressInternal()
	{
		return new InetSocketAddress(csNodeConfig.getNodeAddress(myID), csNodeConfig.getNodePort(myID));
	}
	
	private void sendQueryMesgToContextService(String query, long userReqNum) throws IOException, JSONException
	{
		QueryMsgFromUser<NodeIDType> qmesgU 
			= new QueryMsgFromUser<NodeIDType>(myID, query, sourceIP, LISTEN_PORT, userReqNum);
		
		InetSocketAddress sockAddr = getRandomNodeSock();
		log.trace("Sending query to "+sockAddr);
		niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
	}
	
	private void sendUpdateMesgToContextService(String memberGUID, long userReqNum, String attrName, double newValue) throws IOException, JSONException
	{
		ValueUpdateFromGNS<NodeIDType> updateMesg 
			= new ValueUpdateFromGNS<NodeIDType>(myID, userReqNum, memberGUID, 
					new JSONObject(), sourceIP, LISTEN_PORT, userReqNum );
		
		InetSocketAddress sockAddr = getRandomNodeSock();
		log.trace("Sending update to "+sockAddr);
		niot.sendToAddress(sockAddr, updateMesg.toJSONObject());
	}
	
	private void handleQueryReply(JSONObject jso)
	{
		try
		{
			QueryMsgFromUserReply<NodeIDType> qmur;
			qmur = new QueryMsgFromUserReply<NodeIDType>(jso);
			
			log.trace( "CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					+qmur.getUserReqNum() );
			
			long reqID = qmur.getUserReqNum();
			
			RequestStorageClass reqStorageClass = this.outstandingReq.remove(reqID);
			
			if(reqStorageClass != null)
			{
				JSONArray callerArray = reqStorageClass.getUserGivenArray();
				
				for(int i=0;i<qmur.getResultGUIDs().length();i++)
				{
					callerArray.put(qmur.getResultGUIDs().get(i));
				}
				
				ContextWriterInternals grpWriterInt 
							= reqStorageClass.getMSocketGroupWriterInternals();
				grpWriterInt.setGroupGUID(qmur.getQueryGUID());
				
				// now answer copied in caller JSONArray, notify it.
				synchronized(grpWriterInt.contextServiceQueryWaitLock)
				{
					grpWriterInt.csQueryLockFlag = true;
					grpWriterInt.contextServiceQueryWaitLock.notify();
				}
			}
			else
			{
				System.out.println("reqID "+reqID+" returned NULL "+qmur);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void handleRefreshTrigger(JSONObject jsonObject)
	{
		try
		{
			RefreshTrigger<NodeIDType> refTrig = new RefreshTrigger<NodeIDType>(jsonObject);
			System.out.println("Refresh trigger recvd for "+refTrig.getQuery()+" currTime "+System.currentTimeMillis());
			String query = refTrig.getQuery();
			
			List<ContextWriterInternals> notifierList = this.triggerRegistrations.get(query);
			
			if( notifierList != null )
			{
				for( int i=0; i<notifierList.size();i++ )
				{
					ContextWriterInternals grpWriterInt 
													= notifierList.get(i);
					grpWriterInt.createGroup();
				}
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		BufferedReader reader = null;
		//String line = null;
		// add a leading slash to indicate 'search from the root of the class-path'
		try
		{
		URL configURL = this.getClass().getResource("/" + configFileName);
		InputStream stream = configURL.openStream();
		
		reader = new BufferedReader(new InputStreamReader(stream));
		} catch(Exception ex)
		{
			//ex.printStackTrace();
			reader = new BufferedReader(new FileReader(configFileName));
		}
		String line = null;
		
		while ( (line = reader.readLine()) != null )
		{
			String [] parsed = line.split(" ");
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			nodeList.add(new InetSocketAddress(readIPAddress, readPort));
		}
	}
	
	private InetSocketAddress getRandomNodeSock()
	{
		int size = nodeList.size();
		return nodeList.get( rand.nextInt(size) );
	}
	
	private static void createSingleton() throws IOException
	{
		// doesn't unnecessary acquires lock
		if(thisClass == null)
		{
			synchronized(createSingletonLock)
			{
				if( thisClass == null )
				{
					thisClass = new ContextServiceCallsSingleton<Integer>(0);
				}
			}
		}
	}
	
	private class GroupUpdateThread implements Runnable
	{	
		@Override
		public void run() 
		{
			while(true)
			{
				Set<String> keys = triggerRegistrations.keySet();
		        for(String key: keys)
		        {
		        	
		            //System.out.println("Value of "+key+" is: "+hm.get(key));
		            
		            List<ContextWriterInternals> notifierList = triggerRegistrations.get(key);
					
					if( notifierList != null )
					{
						for( int i=0; i<notifierList.size();i++ )
						{
							ContextWriterInternals grpWriterInt 
															= notifierList.get(i);
							grpWriterInt.createGroup();
						}
					}
		        }
				
				try 
				{
					Thread.sleep(ContextSocketConfig.GROUP_UPDATE_DELAY);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public boolean handleMessage(JSONObject jsonObject)
	{
		log.trace("QuerySourceDemux JSON packet recvd "+jsonObject);
		try
		{
			if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
			{
				handleQueryReply(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.REFRESH_TRIGGER.getInt() )
			{
				handleRefreshTrigger(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
}