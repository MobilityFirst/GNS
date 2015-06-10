package edu.umass.cs.reconfiguration.reconfigurationutils;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

/**
 * @author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
		AbstractJSONPacketDemultiplexer {

	@Override
	public boolean handleMessage(JSONObject json) {
		throw new RuntimeException(
				"This method should never be called unless we have \"forgotten\" to register or handle some packet types.");
	}

}
