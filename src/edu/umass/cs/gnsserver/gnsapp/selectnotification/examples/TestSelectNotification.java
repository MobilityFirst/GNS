package edu.umass.cs.gnsserver.gnsapp.selectnotification.examples;

import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectNotification;

/**
 * A sample implementation of {@link SelectNotification} interface
 * for test purposes.
 * 
 * @author ayadav
 */
public class TestSelectNotification<NotificationType> implements SelectNotification<NotificationType>
{
	private final NotificationType notification;
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
