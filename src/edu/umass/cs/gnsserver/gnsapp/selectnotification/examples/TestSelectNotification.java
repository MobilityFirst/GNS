package edu.umass.cs.gnsserver.gnsapp.selectnotification.examples;

import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectNotification;

/**
 * A sample implementation of {@link SelectNotification} interface
 * for test purposes. * 
 * @author ayadav
 *
 * @param <NotificationType>
 */
public class TestSelectNotification<NotificationType> implements SelectNotification<NotificationType>
{
	private final NotificationType notification;
	
	/**
	 * Creates an object 
	 * @param notification
	 */
	public TestSelectNotification(NotificationType notification)
	{
		this.notification = notification;
	}
	
	@Override
	public NotificationType fromString(String notificationStr) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String toString()
	{
		return this.notification.toString();
	}
}
