package edu.umass.cs.nio;

import edu.umass.cs.gns.util.Util;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * 
 *         This class is a legacy of an old design and currently does not do
 *         very much except for taking bytes from NIO and handing them over to a
 *         suitable packet demultiplexer. Earlier, this class used to have
 *         functionality for parsing the header and payload by buffering the
 *         incoming bytestream by converting it first into a character stream.
 *         This was ugly and the header functionality has now been integrated
 *         into NIOTransport for all messages including byte[].
 */
public class MessageExtractor implements InterfaceMessageExtractor {

	private final ArrayList<AbstractPacketDemultiplexer<?>> packetDemuxes;
	private ScheduledExecutorService executor = Executors
			.newScheduledThreadPool(1); // only for delay emulation

	private static final Logger log = NIOTransport.getLogger();

	protected MessageExtractor(AbstractPacketDemultiplexer<?> pd) {
		packetDemuxes = new ArrayList<AbstractPacketDemultiplexer<?>>();
		packetDemuxes.add(pd);
	}

	protected MessageExtractor() { // default packet demux returns false
		this(new PacketDemultiplexerDefault());
	}

	/**
	 * Note: Use with care. This will change demultiplexing behavior midway,
	 * which is usually not what you want to do. This is typically useful to set
	 * in the beginning.
	 */
	public synchronized void addPacketDemultiplexer(
			AbstractPacketDemultiplexer<?> pd) {
		packetDemuxes.add(pd);
	}

	/**
	 * Incoming data has to be associated with a socket channel, not a nodeID,
	 * because the sending node's id is not known until the message is parsed.
	 * This means that, if the the socket channel changes in the middle of the
	 * transmission, that message will **definitely** be lost.
	 */
	@Override
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		byte[] buf = new byte[incoming.remaining()];
		incoming.get(buf);
		try {
			this.processMessageInternal(socket, buf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		for (AbstractPacketDemultiplexer<?> pd : this.packetDemuxes)
			pd.stop();
		this.executor.shutdownNow();
	}

	@Override
	public void processMessage(InetSocketAddress sockAddr, String msg) {
		try {
			this.demultiplexMessage(sockAddr, sockAddr,
					msg.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING));
		} catch (UnsupportedEncodingException e) {
			fatalExit(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * String to JSON conversion
	 * 
	 * @param msg
	 * @return
	 */
	protected static JSONObject parseJSON(String msg) {
		JSONObject jsonData = null;
		try {
			if (msg.length() > 0) {
				jsonData = new JSONObject(msg);
			}
		} catch (JSONException e) {
			log.severe("Received incorrectly formatted JSON message: " + msg);
			e.printStackTrace();
		}
		return jsonData;
	}

	/* *************** Start of private methods **************************** */

	protected static void fatalExit(UnsupportedEncodingException e) {
		e.printStackTrace();
		System.err.println("NIO failed because the charset encoding "
				+ JSONNIOTransport.NIO_CHARSET_ENCODING
				+ " is not supported; exiting");
		System.exit(1);
	}

	private void processMessageInternal(SocketChannel socket, byte[] msg)
			throws IOException {
		long delay = -1;
		if (JSONDelayEmulator.isDelayEmulated()
				&& (delay = JSONDelayEmulator.getEmulatedDelay(new String(msg,
						MessageNIOTransport.NIO_CHARSET_ENCODING))) >= 0)
			// run in a separate thread after scheduled delay
			executor.schedule(new MessageWorker(socket, msg, packetDemuxes),
					delay, TimeUnit.MILLISECONDS);
		else
			// run it immediately
			this.demultiplexMessage(socket, msg);
	}

	@SuppressWarnings("unchecked")
	protected void demultiplexMessage(InetSocketAddress sender,
			InetSocketAddress receiver, byte[] msg) throws IOException {
		synchronized (this.packetDemuxes) {
			for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
				try {
					if (pd instanceof PacketDemultiplexerDefault)
						continue;

					Object message = pd.getMessage(new String(msg,
							MessageNIOTransport.NIO_CHARSET_ENCODING));
					if (message == null)
						continue;
					// the handler turns true if it handled the message
					else if ((message instanceof JSONObject && (((AbstractPacketDemultiplexer<JSONObject>) pd)
							.handleMessageSuper(stampAddressIntoJSONObject(
									sender, receiver, (JSONObject) message))))
							//
							|| ((message instanceof byte[] && (((AbstractPacketDemultiplexer<byte[]>) pd)
									.handleMessageSuper((byte[]) message))))
							//
							|| ((message instanceof String && (((AbstractPacketDemultiplexer<String>) pd)
									.handleMessageSuper((String) message))))
					// return true if any of the handlers returned true
					)
						return;

				} catch (JSONException je) {
					je.printStackTrace();
				}
			}
		}
	}

	protected void demultiplexMessage(SocketChannel socket, byte[] msg)
			throws IOException {
		this.demultiplexMessage((InetSocketAddress) socket.getRemoteAddress(),
				(InetSocketAddress) socket.getLocalAddress(), msg);
		;
	}

	private class MessageWorker extends TimerTask {

		private final SocketChannel socket;
		private final byte[] msg;

		public MessageWorker(SocketChannel socket, byte[] msg,
				ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes) {
			this.msg = msg;
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				demultiplexMessage(socket, msg);
			} catch (UnsupportedEncodingException e) {
				fatalExit(e);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static JSONObject stampAddressIntoJSONObject(
			InetSocketAddress sndrAddress, InetSocketAddress rcvrAddress,
			JSONObject json) {
		// only put the IP field in if it doesn't exist already
		try {
			// put sender address
			if (!json.has(JSONNIOTransport.SNDR_IP_FIELD))
				json.put(JSONNIOTransport.SNDR_IP_FIELD, sndrAddress
						.getAddress().getHostAddress());
			if (!json.has(JSONNIOTransport.SNDR_PORT_FIELD))
				json.put(JSONNIOTransport.SNDR_PORT_FIELD,
						sndrAddress.getPort());

			// put receiver address
			if (!json.has(JSONNIOTransport.RCVR_IP_FIELD))
				json.put(JSONNIOTransport.RCVR_IP_FIELD, rcvrAddress
						.getAddress().getHostAddress());
			if (!json.has(JSONNIOTransport.RCVR_PORT_FIELD))
				json.put(JSONNIOTransport.RCVR_PORT_FIELD,
						rcvrAddress.getPort());

		} catch (JSONException e) {
			log.severe("Encountered JSONException while stamping sender address and port at receiver: ");
			e.printStackTrace();
		}
		return json;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		assert (false) : "This class' test is not relevant anymore.";
	}
}
