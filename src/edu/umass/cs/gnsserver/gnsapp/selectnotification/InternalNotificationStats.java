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
	
	/**
	 * Creates an object using the totalNotifications parameters.
	 * @param totalNotifications
	 */
	public InternalNotificationStats(int totalNotifications)
	{
		this.totalNotifications = totalNotifications;
		guidsFailed = new HashSet<String>();
	}
	
	
	/**
	 * 
	 * @return Returns the total number of notifications.
	 */
	public int getTotalNotifications()
	{
		return totalNotifications;
	}
	
	/**
	 * 
	 * @return Returns the number of pending notifications.
	 */
	public int getNumberPending()
	{
		return this.numberPending;
	}
	
	/**
	 * 
	 * @return Returns the set of GUIDs to whom the notification sending failed.
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