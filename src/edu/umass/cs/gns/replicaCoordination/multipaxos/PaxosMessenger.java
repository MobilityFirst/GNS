package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;

/**
@author V. Arun
 */
/* This class is separate in order to separate communication from the 
 * paxos protocol. We also probably need to add logging support here
 * where the message is sent only after the logger has confirmed that
 * it is done logging the message persistently.
 */
public class PaxosMessenger {
	private final GNSNIOTransport nioTransport;
	
	Logger log = Logger.getLogger(getClass().getName());
	
	PaxosMessenger(GNSNIOTransport niot) {
		nioTransport = niot;
	}
	
	public void send(MessagingTask mtask) throws JSONException, IOException {
		if(mtask==null || mtask.recipients==null || mtask.msgs==null) return;
		for(int m=0; m<mtask.msgs.length; m++) {
			for(int r=0; r<mtask.recipients.length; r++) {
				if(mtask.msgs[m]==null) continue;
				JSONObject jsonMsg = mtask.msgs[m].toJSONObject();
				nioTransport.sendToID(mtask.recipients[r], jsonMsg);
				log.finest("Sent " + PaxosPacket.typeToString[jsonMsg.getInt(PaxosPacket.ptype)]+ " to node " + mtask.recipients[r] + ": " + jsonMsg);
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
