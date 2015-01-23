package edu.umass.cs.gns.nio;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.gns.nio.nioutils.PacketDemultiplexerDefault;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

/**
 * @author V. Arun
 */

/*
 * We need to read off JSON objects from a byte stream. The problem is
 * that sometimes the stream can miss some bytes. This can happen because
 * say the connection threw an exception and a new one was established.
 * We can reduce the likelihood of missing bytes by writing the remaining
 * bytes as-is on the new connection, but this could result in double-writing
 * bytes sometimes. And there is no way of knowing in this class that the
 * byte stream on the new connection is related to a byte stream on a
 * previous connection. So we have to accept that some messages might get lost
 * because as portions of the byte stream will fail format checks. We can live
 * with this as connection failures are (hopefully) rare.
 * 
 * The more annoying problem that can happen even during graceful execution
 * is that a socket read may return only part of the JSON object, so we don't
 * quite know when we are ready to read a full JSON object. There are a few
 * options to deal with this: (1) Assume each read will include a full JSON
 * object and discard extraneous bytes. (2) If a read only has part of the JSON
 * object, then pass along enough socket or nodeID information to allow the
 * data processor to *synchronously* read more bytes until at least as many
 * bytes as indicated in the header length are obtained. (3) Buffer reads into
 * a local byte stream and process them only when a complete JSON object is
 * available. The first option will likely miss too many objects and possibly
 * even *all* objects if they never align with read boundaries. The second
 * option is complicated and loses the point of the otherwise asynchronous
 * NIO design.
 * 
 * The third option above is therefore the way to go.
 */
public class JSONMessageExtractor implements InterfaceDataProcessingWorker {

	// private static final int SIZE_OF_THREAD_POOL = 100;
	public static final String HEADER_PATTERN = "&"; // Could be an arbitrary string
	private final HashMap<SocketChannel, String> sockStreams;
	private final ArrayList<AbstractPacketDemultiplexer> packetDemuxes;
	private Timer timer = new Timer(); // timer object to schedule packets with delay if we are emulating delays

	Logger log = GNS.getLogger();

	public JSONMessageExtractor(AbstractPacketDemultiplexer pd) {
		packetDemuxes = new ArrayList<AbstractPacketDemultiplexer>();
		packetDemuxes.add(pd);
		sockStreams = new HashMap<SocketChannel, String>();
	}

	public JSONMessageExtractor() { // default packet demux returns false
		this.packetDemuxes = new ArrayList<AbstractPacketDemultiplexer>();
		this.packetDemuxes.add(new PacketDemultiplexerDefault());
		sockStreams = new HashMap<SocketChannel, String>();
	}

	/*
	 * Note: Use with care. This will change demultiplexing behavior
	 * midway, which is usually not what you want to do. This is
	 * useful to set in the beginning.
	 * 
	 * synchronized because it may be invoked when NIO is using
	 * packetDemuxes in processJSONMessage(.).
	 */
	public synchronized void addPacketDemultiplexer(
			AbstractPacketDemultiplexer pd) {
		packetDemuxes.add(pd);
	}

	public synchronized void removePacketDemultiplexer(
			AbstractPacketDemultiplexer pd) {
		packetDemuxes.remove(pd);
	}

	/*
	 * Header is of the form pattern<size>pattern. The pattern is
	 * changeable above. But there is no escape character support
	 * built into the data itself. So if the header pattern occurs
	 * in the data, we could lose a ***lot*** of data, especially
	 * if the bad "size" happens to be a huge value. A big bad size
	 * also means a huge of amount of buffering in SockStreams.
	 */
	public static String prependHeader(String str) {
		return (JSONMessageExtractor.HEADER_PATTERN + str.length() +
				JSONMessageExtractor.HEADER_PATTERN + str);
	}

	/*
	 * Incoming data has to be associated with a socket channel, not a nodeID,
	 * because the sending node's id is not known until the message is parsed.
	 * This means that, if the the socket channel changes in the middle of the
	 * transmission, that message will **definitely** be lost.
	 */
	public void processData(SocketChannel socket, byte[] data, int count) {
		String str = new String(data, 0, count);
		processData(socket, str);
	}

	/*
	 * The above method returns void for backwards compatibility. It is
	 * convenient to test this method if it returns an int.
	 */

	public int processData(SocketChannel socket, String msg) {
		ArrayList<JSONObject> jsonArray =
				this.parseAndSetSockStreams(socket, msg);
		processJSONMessages(jsonArray);
		return jsonArray.size();
	}

	/*
	 * Actual message processing is done by packetDemux. This class only
	 * pre-processes a byte stream into JSON messages.
	 */

	public void processJSONMessages(ArrayList<JSONObject> jsonArray) {
		if (jsonArray != null && !jsonArray.isEmpty()) {
			for (JSONObject jsonMsg : jsonArray) {
				this.processJSONMessage(jsonMsg);
			}
		}
	}

	protected void stop() {
		for (AbstractPacketDemultiplexer pd : this.packetDemuxes)
			pd.stop();
		this.timer.cancel();
	}

	private synchronized void processJSONMessage(final JSONObject jsonMsg) {
		NIOInstrumenter.incrJSONRcvd();

		JsonMessageWorker worker =
				new JsonMessageWorker(jsonMsg, packetDemuxes);
		if (JSONDelayEmulator.isDelayEmulated()) {
			long delay = JSONDelayEmulator.getEmulatedDelay(jsonMsg);
			if (delay >= 0) {
				// this runs in a separate thread after scheduled delay
				timer.schedule(worker, delay);
			} else {
				// THIS ISN'T ACTUALLY RUNNING IT IN ANOTHER THREAD.
				worker.run();
			}
		} else {
			// THIS ISN'T ACTUALLY RUNNING IT IN ANOTHER THREAD.
			worker.run();
		}
	}

	private class JsonMessageWorker extends TimerTask {

		private JSONObject json;
		private ArrayList<AbstractPacketDemultiplexer> pedemuxs;

		public JsonMessageWorker(JSONObject json,
				ArrayList<AbstractPacketDemultiplexer> pedemuxs) {
			this.json = json;
			this.pedemuxs = pedemuxs;
		}

		@Override
		public void run() {
			for (final AbstractPacketDemultiplexer pd : pedemuxs) {
				try {
					if(pd instanceof PacketDemultiplexerDefault) continue;
					// the handler turns true if it handled the message
					if (pd.handleJSONObjectSuper(json))	return;
				} catch(JSONException je) {
					je.printStackTrace();
				}
			}
		}
	}

	/**
	 * *************** Start of private methods *****************************
	 */
	// String to JSON conversion
	private JSONObject parseJSON(String msg) {
		JSONObject jsonData = null;
		try {
			if (msg.length() > 0) {
				jsonData = new JSONObject(msg);
			}
		} catch (JSONException e) {
			log.severe("Received incorrectly formatted JSON message: " +
					jsonData);
			e.printStackTrace();
		}
		return jsonData;
	}

	/*
	 * This method does much of the work to try to extract a single message. Its
	 * purpose is to parse the first correctly header-formatted message and return
	 * it by adding it to jsonArray. The return value is any leftover portion of str.
	 * 
	 * It works as follows. First, it checks if the string length is at least
	 * twice the length of the special pattern in the header. If so, it attempts
	 * to find the first two occurrences of the pattern. If so, it attempts to
	 * parse a size in between those two patterns. If it successfully parses
	 * a size, it checks if the string has at least size bytes beyond the second
	 * occurrence of the special pattern. If so, it has found a correctly header-
	 * formatted message.
	 * 
	 * If the size is not correctly formatted, it gets treated as a 0 size,
	 * so the header will be removed. As a consequence, all bytes up to the
	 * next correctly formatted message will be discarded.
	 */
	private String extractMessage(String str, ArrayList<JSONObject> jsonArray) {
		// GNS.getLogger().info("STR: " + str);
		String retval = str;
		if (str.length() > 2 * JSONMessageExtractor.HEADER_PATTERN.length()) {
			int firstIndex = str.indexOf(JSONMessageExtractor.HEADER_PATTERN);
			int secondIndex =
					str.indexOf(
							JSONMessageExtractor.HEADER_PATTERN,
							firstIndex +
									JSONMessageExtractor.HEADER_PATTERN.length());
			if (firstIndex > -1 && secondIndex > firstIndex) { // found two occurrences of the special pattern
				int size = 0;
				try {
					size =
							Integer.parseInt(str.substring(
									firstIndex +
											JSONMessageExtractor.HEADER_PATTERN.length(),
									secondIndex));
				} catch (NumberFormatException e) {
					log.severe(e.toString());
				}
				/*
				 * Note: If the size is 0 because of an exception above, we
				 * still need to process it as a 0 size message in order to
				 * remove the two special header patterns.
				 */
				int beginMsg =
						secondIndex +
								JSONMessageExtractor.HEADER_PATTERN.length();
				int endMsg =
						secondIndex +
								JSONMessageExtractor.HEADER_PATTERN.length() +
								size;
				if (str.length() >= endMsg) {
					String leftover = str.substring(endMsg);
					String extractedMsg = str.substring(beginMsg, endMsg);
					JSONObject jsonData = this.parseJSON(extractedMsg);
					if (jsonData != null) {
						jsonArray.add(jsonData);
					}
					retval = leftover;
				}
			}
		}
		return retval;
	}

	/* Invokes extractMessage until it can keep extracting more messages */

	private String extractMultipleMessages(String str,
			ArrayList<JSONObject> jsonArray) {
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

	/*
	 * This method will receive new 'data', combine it with existing buffered data in SockStreams
	 * and attempt to extract correctly formatted messages.
	 * 
	 * Synchronized because the hashmap SockStreams is being modified. The parsing internally
	 * has to be synchronized as the result may change the hashmap. The selector thread in NIO
	 * is the only thread that is expected to invoke this method for received messages. But it
	 * is synchronized in case the NIO design is later changed to hand over message processing
	 * to concurrent worker threads. In any case, making it synchronized keeps this class
	 * thread-safe irrespective of whether it is used in conjunction with NIO or not.
	 */
	private synchronized ArrayList<JSONObject> parseAndSetSockStreams(
			SocketChannel sock, String data) {
		String oldStr = sockStreams.get(sock);
		String newStr = (oldStr != null ? oldStr + data : data);
		ArrayList<JSONObject> jsonArray = new ArrayList<JSONObject>();

		newStr = this.extractMultipleMessages(newStr, jsonArray);
		// stamp the senders address and port into the JSON objects
		try {
			stampAddressIntoJSONObjects(((InetSocketAddress) sock.getRemoteAddress()), jsonArray);
		} catch (IOException e) {
			log.severe("Problem getting client address or port: " + e);
		}
		this.sockStreams.put(sock, newStr);
		log.finest("Parsed : [" + JSONArrayToString(jsonArray) +
				"], leftover = [" + newStr + "]");
		return jsonArray;
	}

	// fix this to be enableable
	private void stampAddressIntoJSONObjects(InetSocketAddress address, ArrayList<JSONObject> jsonArray) {
          if (address != null) {
		try {
			for (JSONObject json : jsonArray) {
				// only put the IP field in if it doesn't exist already 
				if (!json.has(JSONNIOTransport.DEFAULT_IP_FIELD)) {
					json.put(JSONNIOTransport.DEFAULT_IP_FIELD, address.getAddress().getHostAddress());
				}
				if (!json.has(JSONNIOTransport.DEFAULT_PORT_FIELD)) {
					json.put(JSONNIOTransport.DEFAULT_PORT_FIELD, address.getPort());
				}
			}
		} catch (JSONException e) {
			log.severe("Unable to stamp sender address and port at receiver: " + e);
		}
          } else {
          log.severe("Address is null, unable to stamp sender address and port at receiver: ");
          }
	}

	// Pretty prints the JSON array, used only for debugging.
	private String JSONArrayToString(ArrayList<JSONObject> jsonArray) {
		String s = "[";
		for (JSONObject jsonData : jsonArray) {
			s += " " + jsonData.toString() + " ";
		}
		return s + "]";
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		JSONMessageExtractor jmw =
				new JSONMessageExtractor(new PacketDemultiplexerDefault());
		String msg = "{\"msg\" : \"Hello  world\"}"; // JSON formatted
		String hMsg = JSONMessageExtractor.prependHeader(msg);
		try {
			SocketChannel sock = SocketChannel.open();

			// Single header-formatted message.
			assert (jmw.processData(sock, hMsg) == 1);
			System.out.println("Expected = " + 1 + ", found = " + 1 +
					" (success)");
			Thread.sleep(1000);

			// Single badly formatted message.
			String bad =
					JSONMessageExtractor.HEADER_PATTERN + "32qw" +
							JSONMessageExtractor.HEADER_PATTERN;
			assert (jmw.processData(sock, bad) == 0);
			System.out.println("Expected = " + 0 + ", found = " + 0 +
					" (success)");
			Thread.sleep(1000);

			// Multiple header-formatted messages back-to-back
			assert (jmw.processData(sock, bad + hMsg) == 1);
			System.out.println("Expected = " + 1 + ", found = " + 1 +
					" (success)");
			Thread.sleep(1000);

			// Multiple header-formatted messages back-to-back
			assert (jmw.processData(sock, hMsg + hMsg) == 2);
			System.out.println("Expected = " + 2 + ", found = " + 2 +
					" (success)");
			Thread.sleep(1000);

			/*
			 * Multiple header-formatted messages with some noise
			 * in the beginning. This can happen, for example, when
			 * a socket channel is renewed because of some exception.
			 */
			assert (jmw.processData(sock, "random noise" + hMsg + hMsg) == 2);
			System.out.println("Expected = " + 2 + ", found = " + 2 +
					" (success)");
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
			System.out.println("Expected = " + msgCount + ", found = " +
					foundCount + " (success)");

			System.out.println("Success: If this is printed, all tests are successful."
					+ " Note that NumberFormatExceptions are expected above.");

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
