package edu.umass.cs.gns.nio;

import org.json.JSONObject;

public abstract class PacketDemultiplexer {

	/**
   * The return value should return true if and only if the multiplexer matched against
   * any of the packet types it was looking for. It doesn't matter if the rest of the
   * processing resulted in an error or not.
	 */
	public abstract boolean handleJSONObject(JSONObject jsonObject);

	public void incrPktsRcvd() {NIOInstrumenter.incrPktsRcvd();} // Used for testing and debugging

}
