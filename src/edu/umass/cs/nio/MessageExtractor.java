package edu.umass.cs.nio;

import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
 * @author V. Arun
 * 
 *         We need to read off String messages from a byte stream. The problem
 *         is that sometimes the stream can miss some bytes. This can happen
 *         because say the connection threw an exception and a new one was
 *         established. We can reduce the likelihood of missing bytes by writing
 *         the remaining bytes as-is on the new connection, but that could
 *         result in double-writing bytes sometimes. And there is no way of
 *         knowing in this class that the byte stream on the new connection is
 *         related to a byte stream on a previous connection. So we have to
 *         accept that some messages might get lost because portions of the
 *         byte stream will fail format checks. We can live with this as
 *         connection failures are (hopefully) rare.
 * 
 *         The more annoying problem that can happen even during graceful
 *         execution is that a socket read may return only part of the message
 *         object, so we don't quite know when we are ready to read a full
 *         message. So we buffer reads into a local byte stream and process them
 *         only when a complete message is available.
 * 
 *         The methods use a consistent charset encoding so that any set of
 *         bytes including binary data can be correctly read.
 * 
 *         Note: Previously, this class was designed primarily for JSON
 *         messages, but it now handles any generic message type by decoding
 *         String messages.
 */
public class MessageExtractor implements InterfaceMessageExtractor {

	/**
	 * The pattern used in the header. The header is the String:
	 * {@code HEADER_PATTERN} + {@code message_size} + {@code HEADER_PATTERN}.
	 */
	public static final String HEADER_PATTERN = "&"; // Could be an arbitrary
														// string
	private final HashMap<SocketChannel, ByteArrayOutputStream> sockStreams;
	private final ArrayList<AbstractPacketDemultiplexer<?>> packetDemuxes;
	private Timer timer = new Timer(MessageExtractor.class.getSimpleName()); // timer object to schedule packets with
										// delay if we are emulating delays

	private static final Logger log = NIOTransport.getLogger();

	protected MessageExtractor(AbstractPacketDemultiplexer<?> pd) {
		packetDemuxes = new ArrayList<AbstractPacketDemultiplexer<?>>();
		packetDemuxes.add(pd);
		sockStreams = new HashMap<SocketChannel, ByteArrayOutputStream>();
	}

	protected MessageExtractor() { // default packet demux returns false
		this(new PacketDemultiplexerDefault());
	}



	/**
	 * Note: Use with care. This will change demultiplexing behavior midway,
	 * which is usually not what you want to do. This is typically useful to set
	 * in the beginning.
	 * 
	 * Synchronized because it may be invoked when NIO is using packetDemuxes in
	 * processJSONMessage(.).
	 */
	public synchronized void addPacketDemultiplexer(
			AbstractPacketDemultiplexer<?> pd) {
		packetDemuxes.add(pd);
	}


	/**
	 * Header is of the form pattern<size>pattern. The pattern is changeable
	 * above. But there is no escape character support built into the data
	 * itself. So if the header pattern occurs in the data, we could lose a
	 * ***lot*** of data, especially if the bad "size" happens to be a huge
	 * value. A big bad size also means a huge of amount of buffering in
	 * SockStreams.
	 */
	protected static String getHeader(String str) {
		return (MessageExtractor.HEADER_PATTERN + str.length() + MessageExtractor.HEADER_PATTERN);
	}

	/**
	 * Incoming data has to be associated with a socket channel, not a nodeID,
	 * because the sending node's id is not known until the message is parsed.
	 * This means that, if the the socket channel changes in the middle of the
	 * transmission, that message will **definitely** be lost.
	 */
	@Override
	public void processData(SocketChannel socket, ByteBuffer incoming) {
		processData(socket, combineNewWithBuffered(socket, incoming));
	}

	/**
	 * The above method returns void for backwards compatibility. It is
	 * convenient to test this method if it returns an int.
	 */

	private int processData(SocketChannel sock, String msg) {
		ArrayList<String> strArray = (this.parseAndSetSockStreams(sock, msg));
		processMessages(getSenderSocketAddress(sock), strArray);
		return strArray.size();
	}

	public void stop() {
		for (AbstractPacketDemultiplexer<?> pd : this.packetDemuxes)
			pd.stop();
		this.timer.cancel();
	}

	public void processMessage(InetSocketAddress sockAddr,
			final String jsonMsg) {
		this.processMessageInternal(sockAddr, jsonMsg);
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
			log.severe("Received incorrectly formatted JSON message: "
					+ jsonData);
			e.printStackTrace();
		}
		return jsonData;
	}

	/* *************** Start of private methods **************************** */

	private String combineNewWithBuffered(SocketChannel socket, ByteBuffer incoming) {
		byte[] data = new byte[incoming.remaining()];
		incoming.get(data);
		try {
			if (!this.sockStreams.containsKey(socket))
				return new String(data, 0, data.length,
						JSONNIOTransport.NIO_CHARSET_ENCODING);
			// else
			this.sockStreams.get(socket).write(data, 0, data.length);
			return this.sockStreams.remove(socket).toString(
					JSONNIOTransport.NIO_CHARSET_ENCODING);

		} catch (UnsupportedEncodingException e) {
			fatalExit(e);
		}
		return null;
	}

	private static void fatalExit(UnsupportedEncodingException e) {
		e.printStackTrace();
		System.err.println("NIO failed because the charset encoding "
				+ JSONNIOTransport.NIO_CHARSET_ENCODING
				+ " is not supported; exiting");
		System.exit(1);
	}

	/*
	 * Actual message processing is done by packetDemux. This class only
	 * pre-processes a byte stream into individual messages.
	 */
	private void processMessages(InetSocketAddress sender,
			ArrayList<String> strArray) {
		if (strArray != null && !strArray.isEmpty()) {
			for (String str : strArray) {
				NIOInstrumenter.incrJSONRcvd(sender.getPort());
				this.processMessage(sender, str);
			}
		}
	}

	private void processMessageInternal(
			InetSocketAddress sockAddr, String msg) {

		MessageWorker worker = new MessageWorker(sockAddr, msg, packetDemuxes);
		long delay = -1;
		if (JSONDelayEmulator.isDelayEmulated()
				&& (delay = JSONDelayEmulator.getEmulatedDelay(msg)) >= 0)
			// run in a separate thread after scheduled delay
			timer.schedule(worker, delay);
		else
			// run it immediately
			worker.run();
	}

	private class MessageWorker extends TimerTask {

		private final InetSocketAddress sockAddr;
		private final String msg;
		private final ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes;

		public MessageWorker(InetSocketAddress sockAddr, String msg,
				ArrayList<AbstractPacketDemultiplexer<?>> pdemuxes) {
			this.msg = msg;
			this.sockAddr = sockAddr;
			this.pdemuxes = pdemuxes;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {

			for (final AbstractPacketDemultiplexer<?> pd : pdemuxes) {
				try {
					if (pd instanceof PacketDemultiplexerDefault)
						continue;

					Object message = pd.getMessage(msg);
					if (message == null)
						continue;
					// the handler turns true if it handled the message
					if (message instanceof JSONObject
							&& (((AbstractPacketDemultiplexer<JSONObject>) pd)
									.handleMessageSuper(stampAddressIntoJSONObject(
											sockAddr, (JSONObject) message))))
						return;
					else if (message instanceof String
							&& (((AbstractPacketDemultiplexer<String>) pd)
									.handleMessageSuper((String) message)))
						return;
					else if (message instanceof byte[]
							&& (((AbstractPacketDemultiplexer<byte[]>) pd)
									.handleMessageSuper((byte[]) message)))
						return;

				} catch (JSONException je) {
					je.printStackTrace();
				}
			}
		}
	}

	/**
	 * This method does much of the work to try to extract a single message. Its
	 * purpose is to parse the first correctly header-formatted message and
	 * return it by adding it to jsonArray. The return value is any leftover
	 * portion of str.
	 * 
	 * It works as follows. First, it checks if the string length is at least
	 * twice the length of the special pattern in the header. If so, it attempts
	 * to find the first two occurrences of the pattern. If so, it attempts to
	 * parse a size in between those two patterns. If it successfully parses a
	 * size, it checks if the string has at least size bytes beyond the second
	 * occurrence of the special pattern. If so, it has found a correctly
	 * header- formatted message.
	 * 
	 * If the size is not correctly formatted, it gets treated as a 0 size, so
	 * the header will be removed. As a consequence, all bytes up to the next
	 * correctly formatted message will be discarded.
	 */
	private String extractMessage(String str, ArrayList<String> msgArray) {
		String retval = str;
		if (str.length() > 2 * MessageExtractor.HEADER_PATTERN.length()) {
			int firstIndex = str.indexOf(MessageExtractor.HEADER_PATTERN);
			int secondIndex = str.indexOf(MessageExtractor.HEADER_PATTERN,
					firstIndex + MessageExtractor.HEADER_PATTERN.length());
			if (firstIndex > -1 && secondIndex > firstIndex) {
				// found two occurrences of the special pattern
				int size = 0;
				try {
					size = Integer.parseInt(str.substring(firstIndex
							+ MessageExtractor.HEADER_PATTERN.length(),
							secondIndex));
				} catch (NumberFormatException e) {
					log.severe(e.toString());
				}
				/*
				 * Note: If the size is 0 because of an exception above, we
				 * still need to process it as a 0 size message in order to
				 * remove the two special header patterns.
				 */
				int beginMsg = secondIndex
						+ MessageExtractor.HEADER_PATTERN.length();
				int endMsg = secondIndex
						+ MessageExtractor.HEADER_PATTERN.length() + size;
				if (str.length() >= endMsg) {
					String leftover = str.substring(endMsg);
					String extractedMsg = str.substring(beginMsg, endMsg);
					msgArray.add(extractedMsg);
					retval = leftover;
				}
			}
		}
		return retval;
	}

	// Invokes extractMessage until it can keep extracting more messages.

	private String extractMultipleMessages(String str,
			ArrayList<String> jsonArray) {
		while (str.length() > 0) {
			String leftover = extractMessage(str, jsonArray);
			if (leftover.equals(str)) {
				break;
			} else {
				str = leftover;
			}
		}
		return str;
	}

	/**
	 * This method will receive new 'data', combine it with existing buffered
	 * data in SockStreams and attempt to extract correctly formatted messages.
	 * 
	 * Synchronized because the hashmap SockStreams is being modified. The
	 * parsing internally has to be synchronized as the result may change the
	 * hashmap. The selector thread in NIO is the only thread that is expected
	 * to invoke this method for received messages. But it is synchronized in
	 * case the NIO design is later changed to hand over message processing to
	 * concurrent worker threads. In any case, making it synchronized keeps this
	 * class thread-safe irrespective of whether it is used in conjunction with
	 * NIO or not.
	 */
	private synchronized ArrayList<String> parseAndSetSockStreams(
			SocketChannel sock, String data) {
		ArrayList<String> jsonArray = new ArrayList<String>();

		String newStr = this.extractMultipleMessages(data, jsonArray);

		this.bufferInSockStream(sock, newStr);
		log.finest("Parsed : [" + JSONArrayToString(jsonArray)
				+ "], leftover = [" + newStr + "]");
		return jsonArray;
	}

	private void bufferInSockStream(SocketChannel sock, String str) {
		byte[] bytes = null;
		try {
			bytes = str.getBytes(JSONNIOTransport.NIO_CHARSET_ENCODING);
		} catch (UnsupportedEncodingException e) {
			fatalExit(e);
		}
		assert (!this.sockStreams.containsKey(sock));
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		byteArray.write(bytes, 0, bytes.length);
		this.sockStreams.put(sock, byteArray);
	}

	private static InetSocketAddress getSenderSocketAddress(SocketChannel sock) {
		InetSocketAddress address = null;
		try {
			address = ((InetSocketAddress) sock.getRemoteAddress());
		} catch (IOException e) {
			log.severe("Unable to get remote sender client address or port to stamp into received packet: "
					+ e);
		}
		return address;
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

	// Pretty prints the JSON array, used only for debugging.
	private String JSONArrayToString(ArrayList<String> jsonArray) {
		String s = "[";
		for (String jsonData : jsonArray) {
			s += " " + jsonData.toString() + " ";
		}
		return s + "]";
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MessageExtractor jmw = new MessageExtractor(
				new PacketDemultiplexerDefault());
		String msg = "{\"msg\" : \"Hello  world\"}"; // JSON formatted
		String hMsg = msg + MessageExtractor.getHeader(msg);
		try {
			SocketChannel sock = SocketChannel.open();

			// Single header-formatted message.
			assert (jmw.processData(sock, hMsg) == 1);
			System.out.println("Expected = " + 1 + ", found = " + 1
					+ " (success)");
			Thread.sleep(1000);

			// Single badly formatted message.
			String bad = MessageExtractor.HEADER_PATTERN + "32qw"
					+ MessageExtractor.HEADER_PATTERN;
			assert (jmw.processData(sock, bad) == 0);
			System.out.println("Expected = " + 0 + ", found = " + 0
					+ " (success)");
			Thread.sleep(1000);

			// Multiple header-formatted messages back-to-back
			assert (jmw.processData(sock, bad + hMsg) == 1);
			System.out.println("Expected = " + 1 + ", found = " + 1
					+ " (success)");
			Thread.sleep(1000);

			// Multiple header-formatted messages back-to-back
			assert (jmw.processData(sock, hMsg + hMsg) == 2);
			System.out.println("Expected = " + 2 + ", found = " + 2
					+ " (success)");
			Thread.sleep(1000);

			/*
			 * Multiple header-formatted messages with some noise in the
			 * beginning. This can happen, for example, when a socket channel is
			 * renewed because of some exception.
			 */
			assert (jmw.processData(sock, "random noise" + hMsg + hMsg) == 2);
			System.out.println("Expected = " + 2 + ", found = " + 2
					+ " (success)");
			Thread.sleep(1000);

			int testParam = 1000;
			int msgCount = 0;
			int foundCount = 0;
			double prob = 0.5;
			for (int i = 0; i < testParam; i++) {
				if (Math.random() > prob) {
					foundCount += jmw.processData(sock, hMsg);
					msgCount++;
				} else {
					foundCount += jmw.processData(sock, "fsdfdsf sdf dsf");
					// Occassionally throw in some crafty bad messages too.
					if (Math.random() > prob) {
						jmw.processData(sock, bad);
					}
				}
			}
			assert (foundCount == msgCount);
			System.out.println("Expected = " + msgCount + ", found = "
					+ foundCount + " (success)");

			System.out
					.println("Success: If this is printed, all tests are successful."
							+ " Note that NumberFormatExceptions are expected above.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
