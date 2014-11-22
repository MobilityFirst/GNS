package edu.umass.cs.gns.gigapaxos.paxosutil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.umass.cs.gns.gigapaxos.multipaxospacket.AcceptReplyPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;
import edu.umass.cs.gns.gigapaxos.multipaxospacket.RequestPacket;
import edu.umass.cs.gns.replicaCoordination.multipaxos.PaxosManager;

/**
@author V. Arun
 */
public class RequestInstrumenter {
	public static final boolean DEBUG = PaxosManager.DEBUG;
	
	private static final HashMap<Integer,String> map = new HashMap<Integer,String>();
	
	private static Logger log = Logger.getLogger(RequestInstrumenter.class.getName()); 
	
	public synchronized static void received(RequestPacket request, int sender, int receiver) {
		map.put(request.requestID, (map.containsKey(request.requestID) ? map.get(request.requestID) : "") + 
			rcvformat(request.requestID, request, sender, receiver));
	}
	public synchronized static void received(AcceptReplyPacket request, int sender, int receiver) {
		map.put(request.getRequestID(), (map.containsKey(request.getRequestID()) ? map.get(request.getRequestID()) : "") + 
			rcvformat(request.getRequestID(), request, sender, receiver));
	}
	public synchronized static void sent(AcceptReplyPacket request, int sender, int receiver) {
		map.put(request.getRequestID(), (map.containsKey(request.getRequestID()) ? map.get(request.getRequestID()) : "") + 
			sndformat(request.getRequestID(), request, sender, receiver));
	}

	public synchronized static void sent(RequestPacket request, int sender, int receiver) {
		map.put(request.requestID, (map.containsKey(request.requestID) ? map.get(request.requestID) : "") + 
			sndformat(request.requestID, request, sender, receiver));		
	}
	
	public synchronized static String remove(int requestID) {
		//if(DEBUG) 
			log.info(requestID + " :\n" + (map.containsKey(requestID) ? map.get(requestID) : ""));
		return map.remove(requestID);
	}
	public synchronized static void removeAll() {
		for(Iterator<Integer> iter = map.keySet().iterator(); iter.hasNext(); ) {
			remove(iter.next());
		}
	}
	
	private synchronized static String rcvformat(int requestID, PaxosPacket packet, int sender, int receiver) {
		return requestID + " " + packet.getType().toString() + " (" + sender + ")   ->" + receiver + " : " + packet.toString() + "\n"; 
	}
	private synchronized static String sndformat(int requestID, PaxosPacket packet, int sender, int receiver) {
		return requestID + " " + packet.getType().toString() + " " + sender + "->   (" + receiver + ") : " + packet.toString() + "\n"; 
	}
}
