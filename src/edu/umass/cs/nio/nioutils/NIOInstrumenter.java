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
package edu.umass.cs.nio.nioutils;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.reconfiguration.testing.ReconfigurableClient;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         Helps instrument read/write stats in NIOTransport. Used for testing
 *         and instrumentation purposes only.
 */

@SuppressWarnings("javadoc")
public class NIOInstrumenter {
	private static int totalSent = 0; // Sent by NIOTransport
	private static int totalRcvd = 0; // Received by NIOTransport
	
	private static int totalBytesSent = 0;
	private static int totalBytesRcvd = 0;

	private static int totalConnAccepted = 0; // NIOTransport
	private static int totalConnInitiated = 0; // NIOTransport
	private static int totalJSONRcvd = 0; // JSONMessageWorker
	private static int totalPktsRcvd = 0; // PacketDemultiplexer
	private static double averageDelay = 0;
	private static boolean enabled = true;
	
	private static Set<Integer> excludePorts = new HashSet<Integer>();
	static {
		excludePorts.add(ReconfigurableClient.TEST_PORT);
	}

	public static synchronized void incrSent() {
		totalSent++;
	}

	public static synchronized void incrRcvd() {
		totalRcvd++;
	}
	
	public static synchronized int incrBytesSent(int sent) {
		return (totalBytesSent+=sent);
	}

	public static synchronized int incrBytesRcvd(int rcvd) {
		return (totalBytesRcvd+=rcvd);
	}

	public static synchronized void incrAccepted() {
		totalConnAccepted++;
	}

	public static synchronized void incrInitiated() {
		totalConnInitiated++;
	}

	public static synchronized void incrJSONRcvd() {
		totalJSONRcvd++;
	}
	public static synchronized boolean incrJSONRcvd(int port) {
		if(!excludePorts.contains(port)) {
			totalJSONRcvd++;
			return true;
		}
		return false;
	}
	public static synchronized boolean incrSent(int port) {
		if(!excludePorts.contains(port)) {
			totalSent++;
			return true;
		}
		return false;
	}

	public static synchronized void incrPktsRcvd() {
		totalPktsRcvd++;
	}

	/**
	 * @param msg
	 * @throws JSONException
	 */
	public static synchronized void rcvdJSONPacket(JSONObject msg)
			throws JSONException {
		if (msg.has(JSONMessenger.SENT_TIME)) {
			averageDelay = Util.movingAverage(
					System.currentTimeMillis()
							- msg.getLong(JSONMessenger.SENT_TIME),
					averageDelay);
		}
	}

	public static synchronized int getMissing() {
		return totalSent - totalJSONRcvd;
	}

	public static synchronized void addExcludePort(int port) {
		excludePorts.add(port);
	}

	public void disable() {
		enabled = (enabled ? false : false);
	}

	public void enable() {
		enabled = true;
	}
	

	public synchronized static String getJSONStats() {
		return "[NIO stats: [ #sent=" + totalSent + " | #rcvd=" + totalJSONRcvd
				+ " | avgNIODelay=" + Util.mu(averageDelay) + "]]";
	}

	public String toString() {
		String s = "";
		return s
				+ "NIO stats: [totalSent = "
				+ totalSent
				+ ", totalRcvd = "
				+ totalRcvd
				+ (totalSent != totalRcvd ? ", missing-or-batched = "
						+ (totalSent - totalRcvd) : "") + "]"
				+ "\n\t [totalConnInitiated = " + totalConnInitiated
				+ ", totalConnAccepted = " + totalConnAccepted + "]"
				+ "\nJSONMessageWorker: [totalJSONRcvd = " + totalJSONRcvd
				+ "]" + "\nDefaultPacketDemultiplexer:  [totalPktsRcvd = "
				+ totalPktsRcvd + "]";
	}
}
