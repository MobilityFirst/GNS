package edu.umass.cs.msocket.contextsocket;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

/**
 * Just a dummy multiplexer
 * @author ayadav
 *
 */
public class ContextServiceDemultiplexer extends AbstractJSONPacketDemultiplexer 
{
	@Override
	public boolean handleMessage(JSONObject message) 
	{
	    return false; // WARNING: Do not change this to true. It could break the GNS by not trying any other PDs.
	}
}