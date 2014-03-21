package edu.umass.cs.gns.nio;

import org.json.JSONObject;

public abstract class PacketDemultiplexer {
	/* FIXME: Change return type to boolean for both methods below, otherwise message 
	 * processing will be inefficient.
	 */
//	public abstract void handleJSONObjects(ArrayList<JSONObject> jsonObjects);

	/* FIXME: Must override this method, otherwise message proecessing will be
	 * inefficient as a single-element ArrayList will be created for each
	 * received message. 
	 */
	public abstract boolean handleJSONObject(JSONObject jsonObject);

	public void incrPktsRcvd() {NIOInstrumenter.incrPktsRcvd();} // Used for testing and debugging

}
