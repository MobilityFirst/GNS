package edu.umass.cs.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.protocoltask.json.ProtocolPacket;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class has support for retransmissions with exponential
 *            backoff. But you can't rely on this backoff for anything other
 *            than ephemeral traffic bursts. If you are overloaded, you are
 *            overloaded, so you must just reduce the load.
 */
@SuppressWarnings("deprecation")
public class JSONMessenger<NodeIDType> implements
		InterfaceJSONNIOTransport<NodeIDType>,
		InterfaceSSLMessenger<NodeIDType, JSONObject> {

	/**
	 * The JSON key for the time when the message was sent. Used only for
	 * instrumentation purposes by
	 * {@link AbstractJSONPacketDemultiplexer#handleMessage(JSONObject)
	 * AbstractPacketDemultiplexer.handleMessage}
	 */
	public static final String SENT_TIME = "SENT_TIME";
	private static final long RTX_DELAY = 1000; // ms
	private static final int BACKOFF_FACTOR = 2;

	private final InterfaceNIOTransport<NodeIDType, JSONObject> nioTransport;
	protected final ScheduledExecutorService execpool;
	private InterfaceAddressMessenger<JSONObject> clientMessenger;

	private Logger log = NIOTransport.getLogger();

	/**
	 * @param niot
	 */
	/**
	 * @param niot
	 */
	public JSONMessenger(InterfaceNIOTransport<NodeIDType, JSONObject> niot) {
		// to not create thread pools unnecessarily
		if (niot instanceof JSONMessenger)
			this.execpool = ((JSONMessenger<NodeIDType>) niot).execpool;
		else
			this.execpool = Executors.newScheduledThreadPool(5);
		nioTransport = (InterfaceNIOTransport<NodeIDType, JSONObject>) niot;
	}

	/**
	 * Send returns void because it is the "ultimate" send. It will retransmit
	 * if necessary. It is inconvenient for senders to worry about
	 * retransmission anyway. We may need to retransmit despite using TCP-based
	 * NIO because NIO is designed to be non-blocking, so it may sometimes drop
	 * messages when asked to send but the channel is congested. We use the
	 * return value of NIO send to decide whether to retransmit.
	 */
	@Override
	public void send(GenericMessagingTask<NodeIDType, ?> mtask)
			throws IOException, JSONException {
		if (mtask == null || mtask.recipients == null || mtask.msgs == null) {
			return;
		}
		for (Object msg : mtask.msgs) {
			if (msg == null) {
				assert (false);
				continue;
			}
			for (int r = 0; r < mtask.recipients.length; r++) {
				JSONObject jsonMsg = null;
				try {
					if (msg instanceof JSONObject) {
						jsonMsg = (JSONObject) (msg);
					} else if (msg instanceof JSONPacket) {
						jsonMsg = ((JSONPacket) (msg)).toJSONObject();
					} else if (msg instanceof ProtocolPacket) {
						jsonMsg = ((ProtocolPacket<?, ?>) msg).toJSONObject();
					} else
						throw new RuntimeException(
								"JSONMessenger received a message that is not of type JSONObject, nio.JSONPacket, or protocoltask.json.ProtocolPacket");
					jsonMsg.put(SENT_TIME, System.currentTimeMillis()); // testing
				} catch (JSONException je) {
					log.severe("JSONMessenger" + getMyID()
							+ " incurred JSONException while decoding: " + msg);
					throw (je);
				}

				int length = jsonMsg.toString().length();

				// special case provision for InetSocketAddress
				int sent = this.specialCaseSend(mtask.recipients[r], jsonMsg);

				// check success or failure and react accordingly
				if (sent == length) {
					log.fine("Node " + this.nioTransport.getMyID() + " sent "
							+ " to node " + mtask.recipients[r] + ": "
							+ jsonMsg);
				} else if (sent < length && sent >= 0) {
					assert(sent==0); // nio buffers all or none
					log.info("Node "
							+ this.nioTransport.getMyID()
							+ " messenger experiencing congestion, this is not disastrous (yet)");
					Retransmitter rtxTask = new Retransmitter(
							(mtask.recipients[r]), jsonMsg, RTX_DELAY);
					// can't block here, so have to ignore returned future
					execpool.schedule(rtxTask, RTX_DELAY, TimeUnit.MILLISECONDS);
				} else {
					log.severe("Node " + this.nioTransport.getMyID() + " sent "
							+ Math.max(-1, sent) + " characters out of a " + length
							+ " message to node " + mtask.recipients[r]);
				}
			}
		}
	}

	// Note: stops underlying NIOTransport as well.
	public void stop() {
		this.execpool.shutdown();
		this.nioTransport.stop();
	}

	@SuppressWarnings("unchecked")
	private int specialCaseSend(Object id, JSONObject json) throws IOException {
		if (id instanceof InetSocketAddress)
			return this.sendToAddress((InetSocketAddress) id, json);
		else
			return this.sendToID((NodeIDType) id, json);
	}

	/**
	 * We need this because NIO may drop messages when congested. Thankfully, it
	 * tells us when it does that. The task below exponentially backs off with
	 * each retransmission. We are probably doomed anyway if this class is
	 * invoked except rarely.
	 */
	private class Retransmitter implements Runnable {

		private final Object dest;
		private final JSONObject msg;
		private final long delay;

		Retransmitter(Object id, JSONObject m, long d) {
			this.dest = id;
			this.msg = m;
			this.delay = d;
		}

		@Override
		public void run() {
			int sent = 0;
			try {
				sent = specialCaseSend(this.dest, this.msg);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} finally {
				if (sent < msg.toString().length() && sent != -1) {
					log.warning("Node "
							+ nioTransport.getMyID()
							+ "->"
							+ dest
							+ " messenger backing off under severe congestion, Hail Mary!");
					Retransmitter rtx = new Retransmitter(dest, msg, delay
							* BACKOFF_FACTOR);
					execpool.schedule(rtx, delay * BACKOFF_FACTOR,
							TimeUnit.MILLISECONDS);
				}
				// queue clogged and !isConnected, best to give up
				else if (sent == -1) {
					log.severe("Node "
							+ nioTransport.getMyID()
							+ "->"
							+ dest
							+ " messenger dropping message as destination unreachable: "
							+ msg);
				}
			}
		}
	}

	/**
	 * Sends jsonData to node id.
	 * 
	 * @param id
	 * @param jsonData
	 * @return Return value indicates the number of bytes sent. A value of -1
	 *         indicates an error.
	 * @throws java.io.IOException
	 */
	@Override
	public int sendToID(NodeIDType id, JSONObject jsonData) throws IOException {
		return this.nioTransport.sendToID(id, jsonData);
	}

	/**
	 * Sends jsonData to address.
	 * 
	 * @param address
	 * @param jsonData
	 * @return Refer {@link #sendToID(Object, JSONObject) sendToID(Object,
	 *         JSONObject)}.
	 * @throws IOException
	 */
	@Override
	public int sendToAddress(InetSocketAddress address, JSONObject jsonData)
			throws IOException {
		return this.nioTransport.sendToAddress(address, jsonData);
	}

	@Override
	public NodeIDType getMyID() {
		return this.nioTransport.getMyID();
	}

	/**
	 * @param pd
	 *            The supplied packet demultiplexer is appended to end of the
	 *            existing list. Messages will be processed by the first
	 *            demultiplexer that has registered for processing the
	 *            corresponding packet type and only by the first demultiplexer.
	 *            <p>
	 *            Note that there is no way to remove demultiplexers. All the
	 *            necessary packet demultiplexers must be determined at design
	 *            time. It is strongly recommended that all demultiplexers
	 *            process exclusive sets of packet types. Relying on the order
	 *            of chained demultiplexers is a bad idea.
	 */
	@Override
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
		this.nioTransport.addPacketDemultiplexer(pd);
	}

	@Override
	public InterfaceAddressMessenger<JSONObject> getClientMessenger() {
		return this.clientMessenger;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setClientMessenger(
			InterfaceAddressMessenger<?> clientMessenger) {
		if (this.clientMessenger != null)
			throw new IllegalStateException(
					"Can not change client messenger once set");
		this.clientMessenger = (InterfaceAddressMessenger<JSONObject>) clientMessenger;
	}
}
