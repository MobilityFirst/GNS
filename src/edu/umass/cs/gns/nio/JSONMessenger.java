package edu.umass.cs.gns.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.PacketInterface;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;

/**
 * @author V. Arun
 */
/*
 * This class is separate in order to separate communication from the
 * paxos protocol. It has support for retransmissions with exponential
 * backoff. But you can't rely on this backoff for anything other than
 * ephemeral traffic bursts. If you are overloaded, you are overloaded.
 */
public class JSONMessenger<NodeIDType> implements InterfaceJSONNIOTransport<NodeIDType> {

	public static final String SENT_TIME = "SENT_TIME"; 
	private static final long RTX_DELAY = 1000; // ms
	private static final int BACKOFF_FACTOR = 2;

	private final InterfaceJSONNIOTransport<NodeIDType> nioTransport;
	private final ScheduledExecutorService execpool =
			Executors.newScheduledThreadPool(5);

	private Logger log =
			(NIOTransport.DEBUG ? Logger.getLogger(getClass().getName())
					: GNS.getLogger());

	public JSONMessenger(InterfaceJSONNIOTransport<NodeIDType> niot) {
		assert (niot != null);
		nioTransport = niot;
	}

	/*
	 * send returns void because it is the "ultimate" send. It will retransmit
	 * if necessary. It is inconvenient for senders to worry about
	 * retransmission anyway. We may need to retransmit despite using
	 * TCP-based NIO because NIO is designed to be non-blocking, so it may
	 * sometimes drop messages when asked to send but the channel is congested.
	 * We use the return value of NIO send to decide whether to retransmit.
	 */
	public void send(GenericMessagingTask<NodeIDType,?> mtask) throws JSONException, IOException {
		if (mtask == null || mtask.recipients == null || mtask.msgs == null) { return; }
		for (Object msg : mtask.msgs) {
			if (msg == null) {
				assert (false);
				continue;
			}
			for (int r = 0; r < mtask.recipients.length; r++) {
				JSONObject jsonMsg = null;
				if (msg instanceof JSONObject) {
					jsonMsg = (JSONObject) (msg);
				}
				else if (msg instanceof PacketInterface) {
					jsonMsg = ((PacketInterface) (msg)).toJSONObject();
				}
				else if (msg instanceof ProtocolPacket){
					log.fine("Messenger received non-JSON object: " + msg);
					jsonMsg = ((ProtocolPacket<?,?>)msg).toJSONObject();
				}
				jsonMsg.put(SENT_TIME, System.currentTimeMillis()); // testing
				
				int length = jsonMsg.toString().length();
				@SuppressWarnings("unchecked")
				int sent = nioTransport.sendToID((NodeIDType)(mtask.recipients[r]), jsonMsg);
				if (sent == length) {
					log.fine("Node " + this.nioTransport.getMyID() + " sent " + " to node " +
							mtask.recipients[r] + ": " + jsonMsg);
				}
				else if (sent < length) {
					if (NIOTransport.sampleLog()) {
						log.warning("Node " + this.nioTransport.getMyID() +
								" messenger experiencing congestion, this is bad but not disastrous (yet)");
					}
					@SuppressWarnings("unchecked")
					Retransmitter rtxTask =
							new Retransmitter((NodeIDType)(mtask.recipients[r]), jsonMsg,
									RTX_DELAY);
					execpool.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS); // can't block, so ignore future
				}
				else {
					log.severe("Node " + this.nioTransport.getMyID() + " sent " + sent +
							" bytes out of a " + length + " byte message");
				}
			}
		}
	}

	// Note: stops underlying NIOTransport as well.
	public void stop() {
		this.execpool.shutdown();
		this.nioTransport.stop();
	}

	/**
	 * ************************* Start of retransmitter module ********************
	 */
	/*
	 * We need this because NIO may drop messages when congested. Thankfully,
	 * it tells us when it does that. The task below exponentially backs
	 * off with each retransmission. We are probably doomed anyway if this
	 * class is invoked except rarely.
	 */
	private class Retransmitter implements Runnable {

		private final NodeIDType dest;
		private final JSONObject msg;
		private final long delay;

		Retransmitter(NodeIDType id, JSONObject m, long d) {
			this.dest = id;
			this.msg = m;
			this.delay = d;
		}

		@Override
		public void run() {
			int sent = 0;
			try {
				sent = nioTransport.sendToID(dest, msg);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally {
				if (sent < msg.toString().length() && sent != -1) {
					log.severe("Node " + nioTransport.getMyID() + "->" + dest +
							" messenger backing off under severe congestion, Hail Mary!");
					Retransmitter rtx =
							new Retransmitter(dest, msg, delay * BACKOFF_FACTOR);
					execpool.schedule(rtx, delay * BACKOFF_FACTOR,
						TimeUnit.MILLISECONDS);
				}
				else if (sent == -1) { // have to give up at this point
					log.severe("Node " +
							nioTransport.getMyID() +
							"->" +
							dest +
							" messenger dropping message as destination unreachable: " +
							msg);
				}
			}
		}
	}

	/**
	 * ************************* End of retransmitter module ********************
	 */

	/**
	 * ******************* Start of GNSNIOTransportInterface methods ********************
	 */

	/**
	 * @param id
	 * @param jsonData
	 * @return
	 * @throws java.io.IOException
	 */
	@Override
	public int sendToID(NodeIDType id, JSONObject jsonData) throws IOException {
		return this.nioTransport.sendToID(id, jsonData);
	}

	/**
	 * 
	 * @param address
	 * @param jsonData
	 * @return
	 * @throws IOException
	 */
	@Override
	public int sendToAddress(InetSocketAddress address, JSONObject jsonData)
			throws IOException {
		return this.nioTransport.sendToAddress(address, jsonData);
	}

	/**
	 * 
	 * @return
	 */
	@Override
	public NodeIDType getMyID() {
		return this.nioTransport.getMyID();
	}
	/**
	 * ******************* End of GNSNIOTransportInterface methods ********************
	 */

	@Override
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
		this.nioTransport.addPacketDemultiplexer(pd);
	}

}
