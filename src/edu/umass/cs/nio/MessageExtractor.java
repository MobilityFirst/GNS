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
package edu.umass.cs.nio;

import edu.umass.cs.nio.interfaces.InterfaceMessageExtractor;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.utils.Util;
import net.minidev.json.JSONValue;

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

	private ArrayList<AbstractPacketDemultiplexer<?>> packetDemuxes;
	private final ScheduledExecutorService executor = Executors
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
		// we update tmp to not have to lock this structure
		ArrayList<AbstractPacketDemultiplexer<?>> tmp = new ArrayList<AbstractPacketDemultiplexer<?>>(
				this.packetDemuxes);
		tmp.add(pd);
		this.packetDemuxes = tmp;
	}

	/**
	 * Incoming data has to be associated with a socket channel, not a nodeID,
	 * because the sending node's id is not known until the message is parsed.
	 * This means that, if the the socket channel changes in the middle of the
	 * transmission, that message will **definitely** be lost.
	 */
	@Override
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		try {
			this.processMessageInternal(socket, incoming);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() {
		for (AbstractPacketDemultiplexer<?> pd : this.packetDemuxes)
			pd.stop();
		this.executor.shutdownNow();
	}

	// called only for loopback receives or by SSL worker
	@Override
	public void processLocalMessage(InetSocketAddress sockAddr, String msg) {
		try {
			this.demultiplexLocalMessage(new NIOHeader(sockAddr, sockAddr), msg);
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
			if (msg.length() > 0)
				jsonData = new JSONObject(msg);
			// Util.toJSONObject(msg);
		} catch (JSONException e) {
			log.severe("Received incorrectly formatted JSON message: " + msg);
			e.printStackTrace();
		}
		return jsonData;
	}

	/**
	 * @param msg
	 * @return Parsed JSON.
	 */
	public static net.minidev.json.JSONObject parseJSONSmart(String msg) {
		net.minidev.json.JSONObject jsonData = null;
		try {
			if (msg.length() > 0)
				jsonData = (net.minidev.json.JSONObject) JSONValue.parse(msg);
		} catch (Exception e) {
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

	// exists only to support delay emulation
	private void processMessageInternal(SocketChannel socket,
			ByteBuffer incoming) throws IOException {
		/*
		 * The emulated delay value is in the message, so we need to read all
		 * bytes off incoming and stringify right away.
		 */
		long delay = -1;
		if (JSONDelayEmulator.isDelayEmulated()) {
			byte[] msg = new byte[incoming.remaining()];
			incoming.get(msg);
			String message = new String(msg,
					MessageNIOTransport.NIO_CHARSET_ENCODING);
			if ((delay = JSONDelayEmulator.getEmulatedDelay(message)) >= 0)
				// run in a separate thread after scheduled delay
				executor.schedule(new MessageWorker(socket, message,
						packetDemuxes), delay, TimeUnit.MILLISECONDS);
		} else
			// run it immediately
			this.demultiplexMessage(
					new NIOHeader(
							(InetSocketAddress) socket.getRemoteAddress(),
							(InetSocketAddress) socket.getLocalAddress()),
					incoming);
	}

	private void demultiplexMessage(NIOHeader header, ByteBuffer incoming)
			throws IOException {
		boolean extracted = false;
		byte[] msg = null;
		// synchronized (this.packetDemuxes)
		{
			for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
				if (pd instanceof PacketDemultiplexerDefault
				// if congested, don't process
						|| pd.isCongested(header))
					continue;

				if (!extracted) { // extract at most once
					msg = new byte[incoming.remaining()];
					incoming.get(msg);
					extracted = true;
				}

				String message = (new String(msg,
						MessageNIOTransport.NIO_CHARSET_ENCODING));
				if (this.callDemultiplexerHandler(header, message, pd))
					return;
			}
		}
	}

	// called only for loopback receives or emulated delays
	private void demultiplexLocalMessage(NIOHeader header, String message)
			throws IOException {
		// synchronized (this.packetDemuxes)
		{
			for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
				if (pd instanceof PacketDemultiplexerDefault)
					// no congestion check
					continue;

				if (this.callDemultiplexerHandler(header, message, pd))
					return;
			}
		}
	}

	// finally called for all receives
	private boolean callDemultiplexerHandler(NIOHeader header, String message,
			AbstractPacketDemultiplexer<?> pd) {
		try {
			// the handler turns true if it handled the message
			if (pd.handleMessageSuper(message, header))
				return true;

		} catch (JSONException je) {
			je.printStackTrace();
		}
		return false;

	}

	@Override
	public void demultiplexMessage(Object message) {
		for (final AbstractPacketDemultiplexer<?> pd : this.packetDemuxes) {
			if (pd.loopback(message))
				break;
		}
	}

	private class MessageWorker extends TimerTask {

		private final SocketChannel socket;
		private final String msg;

		MessageWorker(SocketChannel socket, String msg,
				ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes) {
			this.msg = msg;
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				MessageExtractor.this.demultiplexLocalMessage(new NIOHeader(
						(InetSocketAddress) socket.getRemoteAddress(),
						(InetSocketAddress) socket.getLocalAddress()), msg);
			} catch (UnsupportedEncodingException e) {
				fatalExit(e);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @param sndrAddress
	 * @param rcvrAddress
	 * @param json
	 * @return JSONObject with addresses stamped.
	 */
	public static JSONObject stampAddressIntoJSONObject(
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
	 * For comparing json-smart with org.json.
	 * 
	 * @param sndrAddress
	 * @param rcvrAddress
	 * @param json
	 * @return Parsed JSON object.
	 */
	public static net.minidev.json.JSONObject stampAddressIntoJSONObject(
			InetSocketAddress sndrAddress, InetSocketAddress rcvrAddress,
			net.minidev.json.JSONObject json) {
		// only put the IP field in if it doesn't exist already
		try {
			// put sender address
			if (!json.containsKey(JSONNIOTransport.SNDR_IP_FIELD))
				json.put(JSONNIOTransport.SNDR_IP_FIELD, sndrAddress
						.getAddress().getHostAddress());
			if (!json.containsKey(JSONNIOTransport.SNDR_PORT_FIELD))
				json.put(JSONNIOTransport.SNDR_PORT_FIELD,
						sndrAddress.getPort());

			// put receiver address
			if (!json.containsKey(JSONNIOTransport.RCVR_IP_FIELD))
				json.put(JSONNIOTransport.RCVR_IP_FIELD, rcvrAddress
						.getAddress().getHostAddress());
			if (!json.containsKey(JSONNIOTransport.RCVR_PORT_FIELD))
				json.put(JSONNIOTransport.RCVR_PORT_FIELD,
						rcvrAddress.getPort());

		} catch (Exception e) {
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
