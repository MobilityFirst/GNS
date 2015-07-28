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
	public static final boolean DEBUG = Config.getGlobalBoolean(PC.DEBUG);

	private static final HashMap<Integer, String> map = new HashMap<Integer, String>();

	private static Logger log = Logger.getLogger(RequestInstrumenter.class
			.getName());

	public synchronized static void received(RequestPacket request, int sender,
			int receiver) {
		if (DEBUG)
			map.put(request.requestID,
					(map.containsKey(request.requestID) ? map
							.get(request.requestID) : "")
							+ rcvformat(request.requestID, request, sender,
									receiver));
	}

	public synchronized static void received(AcceptReplyPacket request,
			int sender, int receiver) {
		if (DEBUG)
			map.put(request.getRequestID(),
					(map.containsKey(request.getRequestID()) ? map.get(request
							.getRequestID()) : "")
							+ rcvformat(request.getRequestID(), request,
									sender, receiver));
	}

	public synchronized static void sent(AcceptReplyPacket request, int sender,
			int receiver) {
		if (DEBUG)
			map.put(request.getRequestID(),
					(map.containsKey(request.getRequestID()) ? map.get(request
							.getRequestID()) : "")
							+ sndformat(request.getRequestID(), request,
									sender, receiver));
	}

	public synchronized static void sent(RequestPacket request, int sender,
			int receiver) {
		if (DEBUG)
			map.put(request.requestID,
					(map.containsKey(request.requestID) ? map
							.get(request.requestID) : "")
							+ sndformat(request.requestID, request, sender,
									receiver));
	}

	public synchronized static String remove(int requestID) {
		String retval = map.remove(requestID);
		if (retval != null)
			log.log(Level.FINE, "{0}\n{2}", new Object[] { requestID, retval });
		return retval;
	}

	public synchronized static void removeAll() {
		if (DEBUG)
			for (Iterator<Integer> iter = map.keySet().iterator(); iter
					.hasNext();) {
				remove(iter.next());
			}
	}

	public synchronized static String getLog(int requestID) {
		return map.containsKey(requestID) ? map.get(requestID)
				: "-----------------[" + requestID + ":null]-------";
	}

	private synchronized static String rcvformat(int requestID,
			PaxosPacket packet, int sender, int receiver) {
		return requestID + " " + packet.getType().toString() + " (" + sender
				+ ")   ->" + receiver + " : " + packet.toString() + "\n";
	}

	private synchronized static String sndformat(int requestID,
			PaxosPacket packet, int sender, int receiver) {
		return requestID + " " + packet.getType().toString() + " " + sender
				+ "->   (" + receiver + ") : " + packet.toString() + "\n";
	}
}
