/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 * 
 *         This is a utility class to instrument packet sends and receives to
 *         track problems with paxos requests. This is useful only for
 *         single-node gigapaxos testing.
 */
@SuppressWarnings("javadoc")
public class RequestInstrumenter {
	/**
	 * This class is a no-op unless DEBUG is turned on
	 * */
	@SuppressWarnings("unused")
	public static final boolean DEBUG = false && Config.getGlobalBoolean(PC.DEBUG);

	private static final HashMap<Long, String> map = new HashMap<Long, String>();

	private static Logger log = Logger.getLogger(RequestInstrumenter.class
			.getName());

	public static void received(RequestPacket request, int sender, int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				map.put(request.requestID,
						(map.containsKey(request.requestID) ? map
								.get(request.requestID) : "")
								+ rcvformat(request.requestID, request, sender,
										receiver));
			}
	}

	public static void received(AcceptReplyPacket request, int sender,
			int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				map.put(request.getRequestID(),
						(map.containsKey(request.getRequestID()) ? map
								.get(request.getRequestID()) : "")
								+ rcvformat(request.getRequestID(), request,
										sender, receiver));
			}
	}

	public static void sent(AcceptReplyPacket request, int sender, int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				map.put(request.getRequestID(),
						(map.containsKey(request.getRequestID()) ? map
								.get(request.getRequestID()) : "")
								+ sndformat(request.getRequestID(), request,
										sender, receiver));
			}
	}

	public static void sent(RequestPacket request, int sender, int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				map.put(request.requestID,
						(map.containsKey(request.requestID) ? map
								.get(request.requestID) : "")
								+ sndformat(request.requestID, request, sender,
										receiver));
			}
	}

	public static String remove(long requestID) {
		String retval = null;
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				retval = map.remove(requestID);
			}
		if (retval != null)
			log.log(Level.FINE, "{0}\n{2}", new Object[] { requestID, retval });
		return retval;
	}

	public static void removeAll() {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				for (Iterator<Long> iter = map.keySet().iterator(); iter
						.hasNext();) {
					remove(iter.next());
				}
			}
	}

	public static String getLog(long requestID) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				return map.containsKey(requestID) ? map.get(requestID)
						: "-----------------[" + requestID + ":null]-------";
			}
		return "";
	}

	private static String rcvformat(long requestID, PaxosPacket packet,
			int sender, int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {

				return requestID + " " + packet.getType().toString() + " ("
						+ sender + ")   ->" + receiver + " : "
						+ packet.toString() + "\n";
			}
		return "";
	}

	private static String sndformat(long requestID, PaxosPacket packet,
			int sender, int receiver) {
		if (DEBUG)
			synchronized (RequestInstrumenter.class) {
				return requestID + " " + packet.getType().toString() + " "
						+ sender + "->   (" + receiver + ") : "
						+ packet.toString() + "\n";
			}
		return "";
	}
}
