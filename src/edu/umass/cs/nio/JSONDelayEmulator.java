package edu.umass.cs.nio;

import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;

import org.json.JSONException;
import org.json.JSONObject;

import java.net.SocketAddress;
import java.util.Timer;

/**
 * @author V. Arun
 */

/*
 * This class helps emulate delays in NIO transport. It is mostly
 * self-explanatory. TBD: Need to figure out how to use StartNameServer's
 * emulated latencies.
 * 
 * FIXME: This class works only for Integer/String/InetSocketAddress node IDs as
 * it is essentially a static class.
 */
@SuppressWarnings("unchecked")
public class JSONDelayEmulator {
	/**
	 * The JSON key for the delay value.
	 */
	public static final String DELAY_STR = "_delay";

	private static boolean EMULATE_RECEIVER_DELAYS = false;
	private static double VARIATION = 0.1; // 10% variation in latency
	private static boolean USE_CONFIG_FILE_INFO = false; 
	private static long DEFAULT_DELAY = 100; // 100ms

	private static Object gnsNodeConfig = null; 

	private static final Timer timer = new Timer();

	/**
	 * Enables delay emulation at receiver.
	 */
	public static void emulateDelays() {
		JSONDelayEmulator.EMULATE_RECEIVER_DELAYS = true;
	}

	/**
	 * @param gnsNodeConfig
	 * @param variation
	 */
	public static void emulateConfigFileDelays(GNSNodeConfig<?> gnsNodeConfig,
			double variation) {
		JSONDelayEmulator.EMULATE_RECEIVER_DELAYS = true;
		JSONDelayEmulator.VARIATION = variation;
		JSONDelayEmulator.USE_CONFIG_FILE_INFO = true;
		JSONDelayEmulator.gnsNodeConfig = gnsNodeConfig;
	}

	/**
	 * @return Whether receiver delay emulation is enabled.
	 */
	public static boolean isDelayEmulated() {
		return EMULATE_RECEIVER_DELAYS;
	}

	/**
	 * Stops the underlying timer thread.
	 */
	public static void stop() {
		timer.cancel();
	}

	/**
	 * Sender calls this method to put delay value in the json object before
	 * json object is sent.
	 * 
	 * @param id
	 * @param jsonData
	 */
	public static void putEmulatedDelay(Object id, JSONObject jsonData) {
		if (JSONDelayEmulator.EMULATE_RECEIVER_DELAYS) {
			try {
				jsonData.put(DELAY_STR, getDelay(id));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Receiver calls this method to toString delay that is to be emulated, and
	 * delays processing the packet by that amount. We put the delay to be
	 * emulated inside the json at the sender side because the receiver side
	 * does not know which node ID sent the packet, and hence cannot know how
	 * much to delay the packet
	 * 
	 * @param jsonData
	 * @return The value of the emulated delay.
	 */
	public static long getEmulatedDelay(JSONObject jsonData) {
		if (JSONDelayEmulator.EMULATE_RECEIVER_DELAYS) {
			// if DELAY_STR field is not in json object, we do not emulate delay
			// for that object
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

	/**
	 * @param strData
	 * @return Value of emulated delay.
	 */
	public static long getEmulatedDelay(String strData) {
		JSONObject json = null;
		try {
			json = new JSONObject(strData);
		} catch (JSONException e) {
			return -1;
		}
		return getEmulatedDelay(json);
	}

	private static long getDelay(Object id) {
		long delay = 0;
		if (JSONDelayEmulator.EMULATE_RECEIVER_DELAYS) {
			if (JSONDelayEmulator.USE_CONFIG_FILE_INFO) {
				/*
				 * FIXME: Not sure if we can really support generic types
				 * cleanly in this class as it is mostly static.
				 */
				if (id instanceof Integer)
					// divide by 2 for one-way delay
					delay = ((GNSNodeConfig<Integer>) gnsNodeConfig)
							.getPingLatency((Integer) id) / 2;
				else if (id instanceof String)
					delay = ((GNSNodeConfig<String>) gnsNodeConfig)
							.getPingLatency((String) id) / 2;
				else if (id instanceof SocketAddress)
					delay = ((GNSNodeConfig<SocketAddress>) gnsNodeConfig)
							.getPingLatency((SocketAddress) id) / 2;

			} else {
				delay = JSONDelayEmulator.DEFAULT_DELAY;
			}
			delay = (long) ((1.0 + VARIATION * Math.random()) * delay);
		}
		return delay;
	}

	static class Main {
		public static void main(String[] args) {
			System.out.println("Delay to node " + 3 + " = " + getDelay("3"));
			System.out
					.println("There is no testing code for this class as it is unclear"
							+ " how to access and use ConfigFileInfo. It is unclear what getPingLatency(id) even means."
							+ " How does one specify the source node id?");
		}
	}
}
