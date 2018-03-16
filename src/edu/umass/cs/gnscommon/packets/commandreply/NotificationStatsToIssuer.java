package edu.umass.cs.gnscommon.packets.commandreply;


import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class represents the notification stats for a 
 * select request that is sent back to an issuer.
 * An issuer can be another an entry point name server, which forwards requests
 * to another name servers, or the GNSClient. 
 * 
 * @author ayadav
 *
 */
public class NotificationStatsToIssuer
{
	/**
	 * The keys for serialization of this object into a JSONObject.
	 * @author ayadav
	 *
	 */
	public static enum Keys
	{
		/**
		 * A handle to request the notification status.
		 */
		SELECT_HANDLE,
		/**
		 * Total notifications for a select request at this name server.
		 * At an entry-point name server, this denotes the total notifications 
		 * for a select request, i.e., a cumulative sum of notifications from all name 
		 * servers that are processing and  sending notifications for the select request.
		 */
		TOTAL_NOTIFICATIONS,
		/**
		 * Failed notifications for a select request at this name server.
		 * At an entry-point name server, this denotes the total failed notifications 
		 * for a select request, i.e., a cumulative sum of failed notifications from all name 
		 * servers that are processing and  sending notifications for the select request.
		 */
		FAILED_NOTIFICATIONS,
		/**
		 * Total pending notifications for a select request at this name server.
		 * At an entry-point name server, this denotes the total pending notifications 
		 * for a select request, i.e., a cumulative sum of pending notifications from all name 
		 * servers that are processing and  sending notifications for the select request.
		 */
		PENDING_NOTIFICATIONS,	
	}
	
	private final SelectHandleInfo selectHandle;
	
	private final long totalNotifications;
	private final long failedNotifications;
	private final long pendingNotifications;
	
	/**
	 * The constructor. 
	 * @param selecthandle
	 * The select handle for the issued selectAndNotify GNSCommand. The select handle is 
	 * used to query the status of notifications. 
	 * @param totalNotifications
	 * The total notifications that needs to be sent for the issued selectAndNotify GNSCommand. 
	 * @param failedNotifications
	 * The total failed notifications.
	 * @param pendingNotifications
	 * The total pending notifications. 
	 */
	public NotificationStatsToIssuer(SelectHandleInfo selecthandle, 
			long totalNotifications, long failedNotifications, long pendingNotifications)
	{
		this.selectHandle = selecthandle;
		this.totalNotifications = totalNotifications;
		this.failedNotifications = failedNotifications;
		this.pendingNotifications = pendingNotifications;
	}
	
	/**
	 * 
	 * @return The total notifications that needs to be sent for 
	 * the selectAndNotify GNSCommand. 
	 */
	public long getTotalNotifications()
	{
		return this.totalNotifications;
	}
	
	/**
	 * 
	 * @return The total failed notifications for 
	 * the selectAndNotify GNSCommand.
	 */
	public long getFailedNotifications()
	{
		return this.failedNotifications;
	}
	
	/**
	 * 
	 * @return The total pending notifications for the selectAndNotify GNSCommand.
	 */
	public long getPendingNotifications()
	{
		return this.pendingNotifications;
	}
	
	/**
	 * 
	 * @return The select handle that can be used to query the status of notifications. 
	 */
	public SelectHandleInfo getSelectHandleInfo()
	{
		return this.selectHandle;
	}
	
	/**
	 * Converts the object of this class into a JSONObject
	 * @return The JSONObject corresponding to the object of this class. 
	 * @throws JSONException
	 */
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.SELECT_HANDLE.toString(), selectHandle.toJSONArray());
		json.put(Keys.TOTAL_NOTIFICATIONS.toString(), totalNotifications);
		json.put(Keys.FAILED_NOTIFICATIONS.toString(), failedNotifications);
		json.put(Keys.PENDING_NOTIFICATIONS.toString(), pendingNotifications);
		return json;
	}
	
	/**
	 * To create an object of this class using the supplied JSONObject. 
	 * @param json
	 * @return The object of this class. 
	 * @throws JSONException
	 */
	public static NotificationStatsToIssuer fromJSON(JSONObject json) throws JSONException
	{
		SelectHandleInfo selecthandle = SelectHandleInfo.fromJSONArray
								(json.getJSONArray(Keys.SELECT_HANDLE.toString()));
		long totalNot 	= json.getLong(Keys.TOTAL_NOTIFICATIONS.toString());
		long failedNot 	= json.getLong(Keys.FAILED_NOTIFICATIONS.toString());
		long pendingNot = json.getLong(Keys.PENDING_NOTIFICATIONS.toString());
		
		return new NotificationStatsToIssuer(selecthandle, totalNot, failedNot, pendingNot);
	}	
}