package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import edu.umass.cs.utils.GCConcurrentHashMap;

/**
 * This class stores the select notifications state at a
 * name server.
 * 
 * @author ayadav
 *
 */
public class NameServerSelectNotificationState 
{
	public static final long GC_TIMEOUT			= 5000; // In ms. 
	
	private final HashMap<Long, List<NotificationSendingStats>> notificationInfo;
	
	// For garbage collection.
	// Once there are no pending notifications, then an entry is moved from
	// notificationInfo to garbageCollectionMap, so it is slowly garbage collected.
	private final GCConcurrentHashMap<Long, List<NotificationSendingStats>> garbageCollectionMap;
	
	private final Object lock;
	private final Random rand;
	
	public NameServerSelectNotificationState()
	{
		this.notificationInfo = new HashMap<Long, List<NotificationSendingStats>>();
		garbageCollectionMap = new GCConcurrentHashMap<Long, List<NotificationSendingStats>>(GC_TIMEOUT);
		
		lock = new Object();
		rand = new Random();
		// starting GC thread
		new Thread(new GarbageCollectionThread()).start();
	}
	
	/**
	 * Stores {@code stats} and returns the localHandleId.
	 * This function is thread-safe.
	 * 
	 * @param statsList
	 * @return localHandle
	 * Returns -1 if the addition fails. 
	 */
	public long addNotificationStatsList(List<NotificationSendingStats> statsList)
	{
		if(statsList == null)
			return -1;
		
		synchronized(lock)
		{
			long reqId = -1;
			
			do
			{
				reqId = rand.nextLong();
			}
			while(notificationInfo.containsKey(reqId) 
						|| garbageCollectionMap.containsKey(reqId));
			
			
			notificationInfo.put(reqId, statsList);
			return reqId;
		}
	}
	
	
	public List<NotificationSendingStats> lookupNotificationStats(long localHandle)
	{
		List<NotificationSendingStats> list = notificationInfo.get(localHandle);
		
		if(list == null)
		{
			return this.garbageCollectionMap.get(localHandle);
		}
		else
			return list;
	}
	
	
	private class GarbageCollectionThread implements Runnable
	{
		@Override
		public void run() 
		{
			while(true)
			{
				try 
				{
					Thread.sleep(1000);
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				Iterator<Long> iter = notificationInfo.keySet().iterator();
				List<Long> toBeRemoved = new LinkedList<Long>();
				
				while(iter.hasNext())
				{
					long localId = iter.next();
					if(checkZeroPendingNotifications(notificationInfo.get(localId)))
					{
						toBeRemoved.add(localId);
					}
				}
				
				
				synchronized(lock)
				{
					for(int i=0; i<toBeRemoved.size();i++)
					{
						long localId = toBeRemoved.get(i);
						List<NotificationSendingStats> notList = notificationInfo.remove(localId);
						// putting this entry in garbage collect map. 
						garbageCollectionMap.put(localId, notList);
					}
				}
				
			}
		}
		
		
		private boolean checkZeroPendingNotifications(List<NotificationSendingStats> statList)
		{
			for(int i=0; i<statList.size(); i++)
			{
				NotificationSendingStats notStats = statList.get(i);
				if(notStats.getNumberPending() != 0)
					return false;
			}
			return true;
		}	
	}
}