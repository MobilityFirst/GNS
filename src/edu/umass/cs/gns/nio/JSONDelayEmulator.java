package edu.umass.cs.gns.nio;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 @author V. Arun
 */

/* This class helps emulate delays in NIO transport. It is mostly self-explanatory. 
 * TBD: Need to figure out how to use StartNameServer's emulated latencies.
 * 
 * FIXME: This class works only for Integer node IDs as it is essentially a 
 * static class.
 */
public class JSONDelayEmulator {
  public static final String DELAY_STR = "_delay";

	private static boolean EMULATE_DELAYS = false;
	private static double VARIATION = 0.1; // 10% variation in latency
	private static boolean USE_CONFIG_FILE_INFO = false; // Enable this after figuring out how to use config file
	private static long DEFAULT_DELAY = 100; // 100ms
	private static GNSNodeConfig gnsNodeConfig = null; // node config object to get ping latencies for emulation.


	private static final Timer timer = new Timer();

  /* Use the putEmulatedDelay, and getEmulatedDelay methods for emulation. So, this class is not needed. */
  @Deprecated
	private static class DelayerTask extends TimerTask {

		JSONObject json;
		NodeId<String> destID;
		JSONNIOTransport<NodeId<String>> nioTransport;

		public DelayerTask(JSONNIOTransport<NodeId<String>> nioTransport, NodeId<String> destID, JSONObject json) {
			this.json = json;
			this.destID = destID;
			this.nioTransport = nioTransport;
		}

		@Override
		public void run() {
			try {
				((JSONNIOTransport<NodeId<String>>)nioTransport).sendToIDActual(destID, json);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

  /* Use the putEmulatedDelay, and getEmulatedDelay methods for emulation.
   * Emulating delays at sender side prevents GNSNIOTransport.sendToID from returning
   * the correct return value. */
  @Deprecated
	public static int sendWithDelay(JSONNIOTransport<NodeId<String>> niot, NodeId<String> id, JSONObject jsonData) throws IOException {
		int written = 0;
		if (JSONDelayEmulator.EMULATE_DELAYS) {
			DelayerTask dtask = new DelayerTask(niot, id, jsonData);
			timer.schedule(dtask, getDelay(id));
			written = jsonData.length(); // FIXME: cheating! should deprecate delay emulation
		} else written = niot.sendToIDActual(id, jsonData);
		return written;
	}

	public static void emulateDelays() {
		JSONDelayEmulator.EMULATE_DELAYS = true;
	}

	public static void emulateConfigFileDelays(GNSNodeConfig gnsNodeConfig, double variation) {
		JSONDelayEmulator.EMULATE_DELAYS = true;
		JSONDelayEmulator.VARIATION = variation;
		JSONDelayEmulator.USE_CONFIG_FILE_INFO = true;
		JSONDelayEmulator.gnsNodeConfig = gnsNodeConfig;
	}

	public static boolean isDelayEmulated() {
		return EMULATE_DELAYS;
	}

	public static void stop() {
		timer.cancel();
	}

  /* Sender calls this method to put delay value in the json object before json object is sent.*/
  public static void putEmulatedDelay(NodeId<String> id, JSONObject jsonData) {
    if (JSONDelayEmulator.EMULATE_DELAYS) {
      try {
        jsonData.put(DELAY_STR, getDelay(id));
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }
  }

  /* Receiver calls this method to get delay that is to be emulated, and delays processing the packet by that amount.
  * We put the delay to be emulated inside the json at the sender side because the receiver side does not know which
  * node ID sent the packet, and hence cannot know how much to delay the packet */
  public static long getEmulatedDelay(JSONObject jsonData){
    if (JSONDelayEmulator.EMULATE_DELAYS) {
      // if DELAY_STR field is not in json object, we do not emulate delay for that object
      if (jsonData.has(DELAY_STR)) {
        try {
          return jsonData.getLong(DELAY_STR);
        } catch (JSONException e) {
          e.printStackTrace();
        }
      }
    }
    return -1;
  }


  private static long getDelay(NodeId<String> id) {
		long delay = 0;
		if (JSONDelayEmulator.EMULATE_DELAYS) {
			if (JSONDelayEmulator.USE_CONFIG_FILE_INFO) {
				delay = gnsNodeConfig.getPingLatency(id)/2;// divide by 2 for one-way delay
			} else {
				delay = JSONDelayEmulator.DEFAULT_DELAY;
			}
			delay = (long) ((1.0 + VARIATION * Math.random()) * delay);
		}
		return delay;
	}

	public static void main(String[] args) {
		System.out.println("Delay to node " + 3 + " = " + getDelay(new NodeId<String>(3)));
		System.out.println("There is no testing code for this class as it is unclear"
				+ " how to access and use ConfigFileInfo. It is unclear what getPingLatency(id) even means."
				+ " How does one specify the source node id?");
	}
}
