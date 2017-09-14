package edu.umass.cs.gnscommon.packets.commandreply;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * The local select handle info corresponding to a 
 * selectAndNotify request for a name server.
 * 
 * @author ayadav
 */
public class LocalSelectHandleInfo
{
	public static enum Keys
	{
		/**
		 * A local handleId at a name server for this local select handle. 
		 */
		LOCAL_HANDLE_ID,
		/**
		 * The address of a name server/active for this local select handle.
		 */
		SERVER_ADDRESS,
	}
	
	/**
	 * <serverAddress, localHandleId> is a unique identifier. 
	 */
	private final long localHandleId;
	
	private final InetSocketAddress serverAddress;
	
	
	public LocalSelectHandleInfo(long localHandleId, InetSocketAddress serverAddress)
	{
		this.localHandleId = localHandleId;
		this.serverAddress = serverAddress;
	}
	
	
	public JSONObject toJSONObject() throws JSONException
	{
		JSONObject json = new JSONObject();
		json.put(Keys.LOCAL_HANDLE_ID.toString(), localHandleId);
		json.put(Keys.SERVER_ADDRESS.toString(), 
					serverAddress.getAddress().getHostAddress()+":"+serverAddress.getPort());
		return json;
	}
	
	
	public static LocalSelectHandleInfo fromJSONObject(JSONObject json) throws JSONException
	{
		long handle = json.getLong(Keys.LOCAL_HANDLE_ID.toString());
		String[] ipPort = json.getString(Keys.SERVER_ADDRESS.toString()).split(":");
		
		InetSocketAddress serverAdd = new InetSocketAddress
							(ipPort[0], Integer.parseInt(ipPort[1]));
		
		return new LocalSelectHandleInfo(handle, serverAdd);
	}
	
	/**
	 * Returns the socket address of the name server 
	 * that stores this local select handle.
	 * 
	 * @return
	 */
	public InetSocketAddress getNameServerAddress()
	{
		return this.serverAddress;
	}
	
	
	/**
	 * Returns the local handleId corresponding to 
	 * the local select handle at a name server.
	 * 
	 * @return
	 */
	public long getLocalHandleId()
	{
		return this.localHandleId;
	}
}