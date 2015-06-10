package edu.umass.cs.gigapaxos.paxosutil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;

/**
 * @author V. Arun
 * 
 *         This is a utility class to instrument packet sends and receives to
 *         track problems with paxos requests.
 */
@SuppressWarnings("javadoc")
public class RequestInstrumenter {
	/**
	 * This class is a no-op unless DEBUG is turned on
	 * */
	public static final boolean DEBUG = TESTPaxosConfig.DEBUG;

	private static final HashMap<Integer, String> map = new HashMap<Integer, String>();

	private static Logger log = PaxosManager.getLogger();// Logger.getLogger(RequestInstrumenter.class.getName());

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
		log.log(Level.FINE, "{0}{1}{2}", new Object[] { requestID, " :\n",
				retval });
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
		return map.get(requestID);
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
