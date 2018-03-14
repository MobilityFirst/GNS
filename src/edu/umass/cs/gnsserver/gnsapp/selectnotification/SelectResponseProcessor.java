package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import java.util.List;


/**
 * This interface defines the methods for sending the notifications 
 * from the GNS for a select request. 
 * 
 * @author ayadav
 *
 */
public interface SelectResponseProcessor 
{
	/**
	 * This method is used to implement the logic to send {@code notificationStr}
	 * to {@code guidList}. 
	 * 
	 * @param guidList
	 * The list of GUIDs that satisfy a select query and their associated information
	 * required to send a notification to devices. 
	 * 
	 * @param notificationStr
	 * The notification in string form.
	 * @return
	 * An object of {@link NotificationSendingStats} 
	 * to keep track of notification sending progress.
	 */
	public NotificationSendingStats sendNotification(List<SelectGUIDInfo> guidList, String notificationStr);
}