package edu.umass.cs.gnsserver.gnsapp.selectnotification.examples;

import java.util.List;

import edu.umass.cs.gnsserver.gnsapp.selectnotification.InternalNotificationStats;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.NotificationSendingStats;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectGUIDInfo;
import edu.umass.cs.gnsserver.gnsapp.selectnotification.SelectResponseProcessor;

/**
 * This class is a sample implementation of {@link SelectResponseProcessor} interface.
 * This is mainly used for tests in ant test. In this implementation, 
 * all notifications are always pending. So, in tests we just check that 
 * all notifications are pending to test the end-to-end selectNotify GNSCommand.
 * 
 * @author ayadav
 *
 */
public class PendingSelectResponseProcessor implements SelectResponseProcessor
{	
	public PendingSelectResponseProcessor()
	{
	}
	
	
	@Override
	public NotificationSendingStats sendNotification(List<SelectGUIDInfo> guidList, 
			String notificationStr) 
	{
		InternalNotificationStats internalNot 
					= new InternalNotificationStats(guidList.size());
		internalNot.setNumberPending(guidList.size());
		
		NotificationSendingStats notStats = new NotificationSendingStats(internalNot);
		return notStats;
	}
}