package edu.umass.cs.gns.nio;

import org.json.JSONObject;

/**
@author V. Arun
 */
public interface InterfacePacketDemultiplexer {
	/**
	 * The return value should return true if the handler
	 * handled the message and doesn't want any other BasicPacketDemultiplexer
	 * to handle the message.
	 * 
	 * @param jsonObject
	 * @return
	 */

	public boolean handleJSONObject(JSONObject jsonObject);
}
