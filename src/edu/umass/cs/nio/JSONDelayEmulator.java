/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.nio;

import edu.umass.cs.nio.nioutils.InterfaceDelayEmulator;

import org.json.JSONException;
import org.json.JSONObject;

import java.net.SocketAddress;
import java.util.Timer;

/**
 * @author V. Arun
 */

/*
 * This class helps emulate delays in NIO transport. It is mostly
 * self-explanatory.
 * 
 * FIXME: This class works only for Integer/String/InetSocketAddress node IDs as
 * it is essentially a static class.
 */
public class JSONDelayEmulator {
	/**
	 * The JSON key for the delay value.
	 */
	public static final String DELAY_STR = "_delay";

	private static boolean EMULATE_RECEIVER_DELAYS = false;
	private static double VARIATION = 0.1; // 10% variation in latency
	private static boolean USE_CONFIG_FILE_INFO = false;
	private static long DEFAULT_DELAY = 100; // 100ms

	private static Object pingNodeConfig = null;

	private static final Timer timer = new Timer(JSONDelayEmulator.class.getSimpleName());

	/**
	 * Enables delay emulation at receiver.
	 */
	public static void emulateDelays() {
		JSONDelayEmulator.EMULATE_RECEIVER_DELAYS = true;
	}

	/**
	 * @param pingNodeConfig
	 * @param variation
	 */
	public static void emulateConfigFileDelays(
			InterfaceDelayEmulator<?> pingNodeConfig, double variation) {
		JSONDelayEmulator.EMULATE_RECEIVER_DELAYS = true;
		JSONDelayEmulator.VARIATION = variation;
		JSONDelayEmulator.USE_CONFIG_FILE_INFO = true;
		JSONDelayEmulator.pingNodeConfig = pingNodeConfig;
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

	@SuppressWarnings("unchecked")
	// checked explicitly
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
					delay = ((InterfaceDelayEmulator<Integer>) pingNodeConfig)
							.getEmulatedDelay((Integer) id) / 2;
				else if (id instanceof String)
					delay = ((InterfaceDelayEmulator<String>) pingNodeConfig)
							.getEmulatedDelay((String) id) / 2;
				else if (id instanceof SocketAddress)
					delay = ((InterfaceDelayEmulator<SocketAddress>) pingNodeConfig)
							.getEmulatedDelay((SocketAddress) id) / 2;

			} else {
				delay = JSONDelayEmulator.DEFAULT_DELAY;
			}
			delay = (long) ((1.0 + VARIATION * Math.random()) * delay);
		}
		return delay;
	}
}
