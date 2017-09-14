package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.HashSet;
import java.util.Set;

/**
 * This class keeps notification stats like total notifications, 
 * number of notifications that are still pending, and a set of guids to whom 
 * notifications failed.
 * 
 * @author ayadav
 *
 */
public class InternalNotificationStats 
{
	private final int totalNotifications;
	private int numberPending;
	private Set<String> guidsFailed;
	
	public InternalNotificationStats(int totalNotifications)
	{
		this.totalNotifications = totalNotifications;
		guidsFailed = new HashSet<String>();
	}
	
	
	/**
	 * Returns the total number of notifications.
	 * @return
	 */
	public int getTotalNotifications()
	{
		return totalNotifications;
	}
	
	/**
	 * Returns the number of pending notifications.
	 * @return
	 */
	public int getNumberPending()
	{
		return this.numberPending;
	}
	
	/**
	 * Returns the set of GUIDs to whom the notification sending failed.
	 * @return
	 */
	public Set<String> getGUIDsFailed()
	{
		return this.guidsFailed;
	}
	
	/**
	 * This function sets the number of pending notifications.
	 * This function has to be externally synchronized by its callers.
	 * 
	 * @param numberPending
	 */
	public void setNumberPending(int numberPending)
	{
		this.numberPending = numberPending;
	}
	
	/**
	 * Adds  {@code guid} to the set of failed GUIDs, to whom
	 * the notification sending failed. 
	 * 
	 * @param guid
	 */
	public synchronized void addToGUIDsFailed(String guid)
	{
		guidsFailed.add(guid);
	}
}