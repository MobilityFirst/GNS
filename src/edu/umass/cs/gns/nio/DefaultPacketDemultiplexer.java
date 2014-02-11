package edu.umass.cs.gns.nio;

import java.util.ArrayList;

import org.json.JSONObject;

/**
@author V. Arun
 */

/* Default packet multiplexer simply prints the received JSON object.
 * Note: we have both a DataProcessingWorker and PacketDemultiplexer
 * as the former is for a byte stream and independent of the GNS. The
 * PacketMultiplexer interface is for processing JSON messages. A 
 * GNS-specific DataProcessingWorker would include a GNS-specific
 * packet demultiplexer. This default class is used just for testing.
 */
public class DefaultPacketDemultiplexer extends PacketDemultiplexer {

	public DefaultPacketDemultiplexer() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void handleJSONObjects(ArrayList<JSONObject> jsonObjects) {
		for(JSONObject jo : jsonObjects) {
			System.out.println(jo);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
