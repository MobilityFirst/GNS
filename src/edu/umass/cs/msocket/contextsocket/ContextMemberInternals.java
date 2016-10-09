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
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.HashMap;


import edu.umass.cs.msocket.MServerSocket;
import edu.umass.cs.msocket.MSocket;
import edu.umass.cs.msocket.gns.Integration;
import edu.umass.cs.msocket.logger.MSocketLogger;

public class ContextMemberInternals
{
	//public static final int GET 								= 1;
	public static final int PUT 								= 2;
	public static final int GET_ALL 							= 3;
	
	public static final int QUEUE_POP  							= 1;
	private static final int QUEUE_PUSH 						= 2;
	public static final int QUEUE_SIZE 							= 3;
	
	//private final ExecutorService     readPool;
	
	//private Queue<byte[]> readQueue 							= null;
	
	
	public final Object accptMonitor 							= new Object();
	
	public final Object queueMonitor 							= new Object();
	
	
	private MServerSocket mserversocket 						= null;
	
	private final String localName;
	private final String myGUID;
	
	private HashMap<String, ContextSocket> accptMSockets 		= null;
	private int numAccptSocket						 			= 0;
	
	public ContextMemberInternals(String localName) throws Exception 
	{
		this.localName = localName;
		//LinkedList<InetSocketAddress> proxyList = new LinkedList<InetSocketAddress>();
		//proxyList.add(new InetSocketAddress("ananas.cs.umass.edu", 9189));
		
		//mserversocket = new MServerSocket(localName, new FixedProxyPolicy(proxyList));
		mserversocket = new MServerSocket(localName);
		myGUID = Integration.getGUIDOfAlias(localName);
		
		accptMSockets = new HashMap<String, ContextSocket>();
		
		//readPool = Executors.newCachedThreadPool();
		//readPool = Executors.newFixedThreadPool(POOL_SIZE);
		
		//readQueue = new LinkedList<byte[]>();
		
		//listeningThread = new Thread(new MServerSocketListeningThread());
		//listeningThread.start();
	}
	
	public String getMyGUID()
	{
		return this.myGUID;
	}
	
	public void close()
	{
		//FIXME: not correct close, but closes some threads.
		//active = false;
		try 
		{
			mserversocket.close();
			//readPool.shutdownNow();
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public synchronized Collection<ContextSocket> memberConnectionMapOperations(int typeOfOper, String aliasMember, ContextSocket toPut)
	{
		switch(typeOfOper)
		{
			/*case GET:
			{
				return accptMSockets.get(aliasMember);
			}*/
			case PUT:
			{
				accptMSockets.put(aliasMember, toPut);
				MSocketLogger.getLogger().fine("MSocketGroupMemberInternals new socket accepted "+aliasMember+
						" num sockets "+accptMSockets.size());
				break;
			}
			case GET_ALL:
			{
				return accptMSockets.values();
			}
		}
		return null;
	}
	
	public ContextSocket acceptNewWriter()
	{
		try
		{
			MSocket newJoining = mserversocket.accept();
			numAccptSocket++;
			String key = numAccptSocket +"";
			
			MSocketLogger.getLogger().fine("MSocketGroupMemberInternals new socket accepted "+key);
			ContextSocket readMSocketInf = new ContextSocket(newJoining);
			memberConnectionMapOperations(PUT, key, readMSocketInf);
			
			/*synchronized(accptMonitor) 
			{
				accptMonitor.notifyAll();
			}
			synchronized( queueMonitor) 
			{
				queueMonitor.notifyAll();
			}*/
			
			//FIXME: to be removed
			//readPool.execute(new ReadTask(readMSocketInf));
			return readMSocketInf;
			//accptMSockets.add(newJoining);
		} catch (SocketTimeoutException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/*private class MServerSocketListeningThread implements Runnable
	{
		@Override
		public void run()
		{
			while(active)
			{
				try
				{
					MSocket newJoining = mserversocket.accept();
					numAccptSocket++;
					String key = numAccptSocket +"";
					
					MSocketLogger.getLogger().fine("MSocketGroupMemberInternals new socket accepted "+key);
					ContextSocket readMSocketInf = new ContextSocket(newJoining);
					memberConnectionMapOperations(PUT, key, readMSocketInf);
					
					synchronized(accptMonitor) 
					{
						accptMonitor.notifyAll();
					}
					synchronized( queueMonitor) 
					{
						queueMonitor.notifyAll();
					}
					
					readPool.execute(new ReadTask(readMSocketInf));
					
					//accptMSockets.add(newJoining);
				} catch (SocketTimeoutException e) 
				{
					e.printStackTrace();
				} catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}*/
	
	/*public synchronized Object readQueueOperations(int typeOper, byte[] toPut) 
	{
		switch(typeOper) 
		{
			case QUEUE_POP: 
			{
				return readQueue.poll();
			}
			case QUEUE_PUSH:
			{
				readQueue.add(toPut);
				
				break;
			}
			case QUEUE_SIZE: 
			{
				return readQueue.size();
			}
		}
		return null;
	}*/
}