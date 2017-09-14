package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.Set;

/**
 * This class keeps track of different statistics while sending notifications.
 * The different statistics are automatically updated based on the progress in
 * notification sending.
 * 
 * @author ayadav
 *
 */
public class NotificationSendingStats 
{
	private final InternalNotificationStats internalStats;
	
	public NotificationSendingStats(InternalNotificationStats internalStats)
	{
		this.internalStats = internalStats;
	}
	
	/**
	 * Returns the total number of notifications.
	 * @return
	 */
	public int getTotalNotifications()
	{
		return internalStats.getTotalNotifications();
	}
	
	/**
	 * Returns the number of pending notifications.
	 * A call to this method returns the recent number of pending notifications.
	 * @return
	 */
	public int getNumberPending()
	{
		return internalStats.getNumberPending();
	}
	
	/**
	 * Returns the set of GUIDs to whom the notification sending failed.
	 * A call to this method returns the recent set of guids to whom notification
	 * sending failed. 
	 * @return
	 */
	public Set<String> getGUIDsFailed()
	{
		return internalStats.getGUIDsFailed();
	}
}
