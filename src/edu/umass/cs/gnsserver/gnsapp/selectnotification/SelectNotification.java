package edu.umass.cs.gnsserver.gnsapp.selectnotification;

/**
 * This interface defines the methods that every select notification 
 * class should implement.
 * 
 * @author ayadav
 *
 * @param <NotificationType>
 */
public interface SelectNotification<NotificationType>
{
	/**
	 * This method converts a notification into a string form.
	 * A GNSClient converts a notification into a string form and then
	 * sends it to GNS servers in a select request. 
	 * @return 
	 * The notification in string form.
	 */
	public String toString();
	
	
	/**
	 * This method converts a notification in string form to a
	 * NotificationType.  
	 * 
	 * @param notificationStr
	 * A notification in string form.
	 * @return
	 * A notification of type NotificationType
	 */
	public NotificationType fromString(String notificationStr);
}