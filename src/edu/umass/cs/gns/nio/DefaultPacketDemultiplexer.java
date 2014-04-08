package edu.umass.cs.gns.nio;

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
  public boolean handleJSONObject(JSONObject jsonObject) {
    incrPktsRcvd();
    //System.out.println("Received pkt: " + jsonObject);
    return true;
  }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
