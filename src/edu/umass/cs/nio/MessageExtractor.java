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
			this.processMessageInternal(
					(InetSocketAddress) socket.getRemoteAddress(), buf);
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
			// FIXME: needless string->bytes->string conversion for local sends
			this.processMessageInternal(sockAddr,
					msg.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING));
		} catch (UnsupportedEncodingException e) {
			fatalExit(e);
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

	private void processMessageInternal(InetSocketAddress sockAddr, byte[] msg)
			throws UnsupportedEncodingException {
		long delay = -1;
		if (JSONDelayEmulator.isDelayEmulated()
				&& (delay = JSONDelayEmulator.getEmulatedDelay(new String(msg,
						MessageNIOTransport.NIO_CHARSET_ENCODING))) >= 0)
			// run in a separate thread after scheduled delay
			executor.schedule(new MessageWorker(sockAddr, msg, packetDemuxes),
					delay, TimeUnit.MILLISECONDS);
		else
			// run it immediately
			this.demultiplexMessage(sockAddr, msg);
	}

	@SuppressWarnings("unchecked")
	protected void demultiplexMessage(InetSocketAddress sockAddr, byte[] msg)
			throws UnsupportedEncodingException {
		synchronized (this.packetDemuxes) 
		{
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
									sockAddr, (JSONObject) message))))
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

	private class MessageWorker extends TimerTask {

		private final InetSocketAddress sockAddr;
		private final byte[] msg;

		public MessageWorker(InetSocketAddress sockAddr, byte[] msg,
				ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes) {
			this.msg = msg;
			this.sockAddr = sockAddr;
		}

		@Override
		public void run() {
			try {
				demultiplexMessage(sockAddr, msg);
			} catch (UnsupportedEncodingException e) {
				fatalExit(e);
			}
		}
	}

	private static JSONObject stampAddressIntoJSONObject(
			InetSocketAddress address, JSONObject json) {
		// only put the IP field in if it doesn't exist already
		try {
			if (!json.has(JSONNIOTransport.DEFAULT_IP_FIELD))
				json.put(JSONNIOTransport.DEFAULT_IP_FIELD, address
						.getAddress().getHostAddress());
			if (!json.has(JSONNIOTransport.DEFAULT_PORT_FIELD))
				json.put(JSONNIOTransport.DEFAULT_PORT_FIELD, address.getPort());

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
