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
	
	/**
	 * Creates an object using {@link InternalNotificationStats}.
	 * The supplied object of {@link InternalNotificationStats} gets updated
	 * in the background based on the notification sending progress. 
	 * @param internalStats
	 */
	public NotificationSendingStats(InternalNotificationStats internalStats)
	{
		this.internalStats = internalStats;
	}
	
	/**
	 * @return Returns the total number of notifications.
	 */
	public int getTotalNotifications()
	{
		return internalStats.getTotalNotifications();
	}
	
	/**
	 * A call to this method returns the recent number of pending notifications.
	 * @return Returns the number of pending notifications.
	 */
	public int getNumberPending()
	{
		return internalStats.getNumberPending();
	}
	
	/**
	 * 
	 * A call to this method returns the recent set of guids to whom notification
	 * sending failed. 
	 * @return Returns the set of GUIDs to whom the notification sending failed.
	 */
	public Set<String> getGUIDsFailed()
	{
		return internalStats.getGUIDsFailed();
	}
}
