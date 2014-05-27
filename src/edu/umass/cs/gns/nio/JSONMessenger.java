package edu.umass.cs.gns.nio;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.nsdesign.packet.PacketInterface;

/**
@author V. Arun
 */
/* This class is separate in order to separate communication from the 
 * paxos protocol. It has support for retransmissions with exponential
 * backoff. But you can't rely on this backoff for anything other than
 * ephemeral traffic bursts. If you are overloaded, you are overloaded.
 * 
 */
public class JSONMessenger {
	private static final long RTX_DELAY = 1000; //ms
	private static final int BACKOFF_FACTOR=2; 

	private final int myID;
	private final GNSNIOTransportInterface nioTransport;
	private ScheduledExecutorService execpool = Executors.newScheduledThreadPool(5);

	Logger log = Logger.getLogger(getClass().getName());

	public JSONMessenger(int id, GNSNIOTransportInterface niot) {
		myID = id; // needed only for debug printing
		nioTransport = niot;
	}

	/* send returns void because it is the "ultimate" send. It will retransmit
	 * if necessary. It is inconvenient for senders to worry about 
	 * retransmission anyway. We may need to retransmit despite using 
	 * TCP-based NIO because NIO is designed to be non-blocking, so it may 
	 * sometimes drop messages when asked to send but the channel is congested. 
	 * We use the return value of NIO send to decide whether to retransmit.
	 */
	public void send(MessagingTask mtask) throws JSONException, IOException {
		if(mtask==null || mtask.recipients==null || mtask.msgs==null) return;
		for(int m=0; m<mtask.msgs.length; m++) {
			for(int r=0; r<mtask.recipients.length; r++) {
				if(mtask.msgs[m]==null) {assert(false); continue;}

				JSONObject jsonMsg = null;
				if(mtask.msgs[m] instanceof JSONObject)
					jsonMsg = (JSONObject)(mtask.msgs[m]);
				else if(mtask.msgs[m] instanceof PacketInterface)
					jsonMsg = ((PacketInterface)(mtask.msgs[m])).toJSONObject();
				else {
					log.warning("Messenger received non-JSON object: " + mtask.msgs[m]);
					assert(false); continue; 
				}

				int sent = nioTransport.sendToID(mtask.recipients[r], jsonMsg);
				if(sent==jsonMsg.length()) {
					log.finest("Node "+this.myID+" sent " + " to node " + mtask.recipients[r] + ": " + jsonMsg);
				} else if (sent < jsonMsg.length()) {
					if(NIOTransport.sampleLog())
						log.warning("Node "+this.myID+" messenger experiencing congestion, this is bad but not disastrous (yet)");
					Retransmitter rtxTask = new Retransmitter(mtask.recipients[r], jsonMsg, RTX_DELAY);
					execpool.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS); // can't block, so ignore returned future
				}
			}
		}
	}

	/* We need this because NIO may drop messages when congested. Thankfully, 
	 * it tells us when it does that. The task below exponentially backs
	 * off with each retransmission. We are probably doomed anyway if this 
	 * class is invoked except rarely.
	 */
	private class Retransmitter implements Runnable {
		private final int dest;
		private final JSONObject msg;
		private final long delay;
		Retransmitter(int id, JSONObject m, long d) {
			this.dest=id;
			this.msg=m;
			this.delay=d;
		}
		public void run() {
			int sent=0;
			try {
				sent = nioTransport.sendToID(dest, msg);
			} catch(IOException ioe) {
				ioe.printStackTrace();
			} finally {
				if(sent < msg.length() && sent!=-1) { 
					log.severe("Node "+myID +"->"+dest+" messenger backing off under severe congestion, Hail Mary!");
					Retransmitter rtx = new Retransmitter(dest, msg, delay*BACKOFF_FACTOR);
					execpool.schedule(rtx, delay*BACKOFF_FACTOR, TimeUnit.MILLISECONDS);
				} else if(sent==-1) { // have to give up at this point
					log.severe("Node "+myID +"->"+dest+" messenger dropping message as destination unreachable: " + msg);
				}
			}
		}
	}
}
