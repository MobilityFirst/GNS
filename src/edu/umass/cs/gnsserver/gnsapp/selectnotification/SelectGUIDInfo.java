package edu.umass.cs.gnsserver.gnsapp.selectnotification;

import org.json.JSONObject;

/**
 * An object of this class stores the information 
 * about GUIDs that satisfy a select request. 
 * This class can be used to include the IPaddress:port or 
 * other deviceID information corresponding to a GUID that is required 
 * to eventually send a notification to that GUID. 
 * 
 * @author ayadav
 *
 */
public class SelectGUIDInfo
{
	private final String guid;
	private final JSONObject keyValuePairs;
	
	/**
	 * Creates the object of this class. 
	 * @param guid
	 * @param keyValuePairs
	 */
	public SelectGUIDInfo(String guid, JSONObject keyValuePairs)
	{
		this.guid = guid;
		this.keyValuePairs = keyValuePairs;
	}
	
	/**
	 * 
	 * @return The associated GUID.
	 */
	public String getGUID()
	{
		return this.guid;
	}
	
	/**
	 * 
	 * @return The request key-value pairs in a select request. 
	 */
	public JSONObject getKeyValuePairs()
	{
		return this.keyValuePairs;
	}
}