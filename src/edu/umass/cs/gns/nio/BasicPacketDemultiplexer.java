package edu.umass.cs.gns.nio;

import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.Packet;

public abstract class BasicPacketDemultiplexer {
	
	/********************************** Start of new, untested parts ***************************/
	ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10); // FIXME: Not sure on what basis to limit number of threads
	HashMap<Packet.PacketType, Boolean> demuxMap = new HashMap<Packet.PacketType, Boolean>();

	public boolean register(Packet.PacketType type) {
		boolean isFirst = this.demuxMap.containsKey(type);
		this.demuxMap.put(type, true);
		return isFirst;
	}
	public boolean handleJSONObjectSuper(JSONObject jsonObject) throws JSONException {
		Tasker tasker = new Tasker(jsonObject);
		boolean handled = this.demuxMap.containsKey(Packet.getPacketType(jsonObject));
		if(handled) executor.schedule(tasker, 0, TimeUnit.MILLISECONDS);
		return handled;
	}
	
	private class Tasker implements Runnable {
		private final JSONObject json;
		Tasker(JSONObject json) {
			this.json = json;
		}
		public void run() {
			try {
				handleJSONObjectSuper(this.json);
			} catch(JSONException je) {
				GNS.getLogger().severe("Can not parse packet type in " + this.json);
				je.printStackTrace();
			}
		}
	}
	/********************************** End of new, untested parts ***************************/

	/**
	 * The return value should return true if the handler
	 * handled the message and doesn't want any other BasicPacketDemultiplexer
	 * to handle the message.
	 *
	 * @param jsonObject
	 * @return
	 */
	public abstract boolean handleJSONObject(JSONObject jsonObject);

	public void incrPktsRcvd() {
		NIOInstrumenter.incrPktsRcvd();
	} // Used for testing and debugging
}
