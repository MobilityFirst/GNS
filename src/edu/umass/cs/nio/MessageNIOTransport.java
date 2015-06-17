package edu.umass.cs.nio;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.nio.nioutils.PacketDemultiplexerDefault;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <MessageType>
 * 
 *            This class exists primarily as a wrapper around NIOTransport to
 *            support messages. NIOTransport is for general-purpose NIO byte
 *            stream communication between numbered nodes as specified by the
 *            NodeConfig interface and a data processing worker as specified by
 *            the DataProcessingWorker interface that handles a byte stream.
 *            This class provides the abstraction of messages and a
 *            corresponding PacketDemultiplexer that handles messages instead of
 *            a continuous byte stream.
 * 
 *            MessageType can be any type whose toString() method results in a
 *            meaningful serialization of a MessageType instance, i.e., the
 *            packet demultiplexer on the other end must be able to reconstruct
 *            the MessageType object from the String. The one exception is
 *            byte[] that is supported here. However, MessageType can not extend
 *            Object[] or be any primitive array other than byte[]. If byte[] as
 *            MessageType, the corresponding
 *            AbstractPacketDemultiplexer.getMessage(byte[]) method should
 *            return a String decoded from the byte[] assuming ISO-8859-1
 *            encoding.
 * 
 *            This class also supports delay emulation and short-circuiting
 *            local sends by directly sending it to the packet demultiplexer.
 */

public class MessageNIOTransport<NodeIDType, MessageType> extends
		NIOTransport<NodeIDType> implements
		InterfaceNIOTransport<NodeIDType, MessageType> {

	/**
	 * JSON key corresponding to sender IP address. Relevant only if
	 * {@code MessageType} is JSONObject.
	 */
	public static final String DEFAULT_IP_FIELD = "_IP_ADDRESS";
	/**
	 * JSON key corresponding to sender port number. Relevant only if
	 * {@code MessageType} is JSONObject.
	 */

	public static final String DEFAULT_PORT_FIELD = "_TCP_PORT";

	/**
	 * Initiates transporter with id and nodeConfig.
	 * 
	 * @param id
	 *            My node ID.
	 * @param nodeConfig
	 *            A map from all nodes' IDs to their respective socket
	 *            addresses.
	 * @throws IOException
	 */
	public MessageNIOTransport(NodeIDType id,
			InterfaceNodeConfig<NodeIDType> nodeConfig) throws IOException {
		// Note: Default extractor will not do any useful demultiplexing
		super(id, nodeConfig, new MessageExtractor());
	}

	/**
	 * @param id
	 * @param nodeConfig
	 * @param sslMode
	 * @throws IOException
	 */
	public MessageNIOTransport(NodeIDType id,
			InterfaceNodeConfig<NodeIDType> nodeConfig,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		// Note: Default extractor will not do any useful demultiplexing
		super(id, nodeConfig, new MessageExtractor(), true, sslMode);
	}

	/**
	 * 
	 * @param id
	 *            My node ID.
	 * @param nodeConfig
	 *            A map from all nodes' IDs to their respective socket
	 *            addresses.
	 * @param pd
	 *            The packet demultiplexer to handle received messages.
	 * @param start
	 *            If a server thread must be automatically started upon
	 *            construction. If false, the caller must explicitly invoke (new
	 *            Thread(JSONNIOTransport)).start() to start the server.
	 * @throws IOException
	 */
	public MessageNIOTransport(NodeIDType id,
			InterfaceNodeConfig<NodeIDType> nodeConfig,
			AbstractPacketDemultiplexer<?> pd, boolean start)
			throws IOException {
		// Switched order of the latter two arguments
		super(id, nodeConfig, new MessageExtractor(pd));
		if (start && !isStarted()) {
			(new Thread(this)).start();
		}
	}

	/**
	 * @param id
	 * @param nodeConfig
	 * @param pd
	 * @param start
	 * @param sslMode
	 *            To enable SSL.
	 * @throws IOException
	 */
	public MessageNIOTransport(NodeIDType id,
			InterfaceNodeConfig<NodeIDType> nodeConfig,
			AbstractPacketDemultiplexer<?> pd, boolean start,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		// Switched order of the latter two arguments
		super(id, nodeConfig, new MessageExtractor(pd), start, sslMode);
	}
	
	/**
	 * @param address
	 * @param port
	 * @param pd
	 * @param sslMode
	 * @throws IOException
	 */
	public MessageNIOTransport(InetAddress address, int port,
			AbstractPacketDemultiplexer<?> pd, SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		super(address, port, new MessageExtractor(pd), sslMode);
	}

	/**
	 * Used only for testing. The private nature of this method means that
	 * external users can no longer explicitly specify the message extractor
	 * that can now only be {@link edu.umass.cs.nio.MessageExtractor}.
	 * 
	 * @param id
	 *            My node ID.
	 * @param nodeConfig
	 *            A map from all nodes' IDs to their respective socket
	 *            addresses.
	 * @param worker
	 *            The message extractor.
	 * 
	 * @throws IOException
	 */
	private MessageNIOTransport(NodeIDType id,
			InterfaceNodeConfig<NodeIDType> nodeConfig, MessageExtractor worker)
			throws IOException {
		// Switched order of the latter two arguments
		super(id, nodeConfig, worker);
	}

	/**
	 * @param pd
	 *            The demultiplxer to add to the current chain.
	 */
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
		// will throw exception if worker not MessageExtractor
		((InterfaceMessageExtractor) this.worker).addPacketDemultiplexer(pd);
	}

	/**
	 * 
	 */
	@Override
	public NodeIDType getMyID() {
		return this.myID;
	}

	public void stop() {
		super.stop();
		((InterfaceMessageExtractor) this.worker).stop();
		JSONDelayEmulator.stop();
	}

	/**
	 * Send a JSON packet to a node id.
	 *
	 * @param id
	 * @param msg
	 * @return Refer {@link JSONMessenger#sendToID(Object, JSONObject)
	 *         JSONMessenger.sendToID(Object, JSONObject)}.
	 * @throws IOException
	 */
	@Override
	public int sendToID(NodeIDType id, MessageType msg) throws IOException {
		if (JSONDelayEmulator.isDelayEmulated() && msg instanceof JSONObject)
			JSONDelayEmulator.putEmulatedDelay(id, (JSONObject) msg);
		return sendToIDInternal(id, msg);
	}

	/**
	 * Send a JSON packet to an inet socket address (ip and port).
	 *
	 * @param isa
	 * @param msg
	 * @return Refer {@link JSONMessenger#sendToID(Object, JSONObject)
	 *         JSONMessenger.sendToID(Object, JSONObject)}.
	 * @throws IOException
	 */
	@Override
	public int sendToAddress(InetSocketAddress isa, MessageType msg)
			throws IOException {
		if (msg instanceof byte[])
			return this.stampHeaderAndSend(isa, (byte[]) msg);
		return this.stampHeaderAndSend(isa, msg.toString());
	}

	/**
	 * @param json
	 * @return Socket address of the sender recorded in this JSON message at
	 *         receipt time.
	 */
	public static InetSocketAddress getSenderAddress(JSONObject json) {
		try {
			InetAddress address = (json
					.has(MessageNIOTransport.DEFAULT_IP_FIELD) ? InetAddress
					.getByName(json.getString(
							MessageNIOTransport.DEFAULT_IP_FIELD).replaceAll(
							"[^0-9.]*", "")) : null);
			int port = (json.has(MessageNIOTransport.DEFAULT_PORT_FIELD) ? json
					.getInt(MessageNIOTransport.DEFAULT_PORT_FIELD) : -1);
			if (address != null && port > 0) {
				return new InetSocketAddress(address, port);
			}
		} catch (UnknownHostException | JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param json
	 * @return Sender InetAddress read from json.
	 * @throws JSONException
	 */
	public static final InetAddress getSenderInetAddress(JSONObject json)
			throws JSONException {
		if (json.has(JSONNIOTransport.DEFAULT_IP_FIELD)) {
			try {
				return InetAddress.getByName(json.getString(
						JSONNIOTransport.DEFAULT_IP_FIELD).replaceAll(
						"[^0-9.]*", ""));
			} catch (UnknownHostException uhe) {
				uhe.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * @param json
	 * @return InetAddress of sender recorded at time of receipt as string.
	 */
	public static String getSenderInetAddressAsString(JSONObject json) {
		try {
			String address = (json.has(MessageNIOTransport.DEFAULT_IP_FIELD) ? (json
					.getString(MessageNIOTransport.DEFAULT_IP_FIELD)
					.replaceAll("[^0-9.]*", "")) : null);
			return address;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param json
	 * @return Port number of sender socket address recorded at time of receipt.
	 */
	public static int getSenderPort(JSONObject json) {
		try {
			int port = (json.has(MessageNIOTransport.DEFAULT_PORT_FIELD) ? (json
					.getInt(MessageNIOTransport.DEFAULT_PORT_FIELD)) : -1);
			return port;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return -1;
	}

	/* ******************End of public send methods************************** */

	/*
	 * This method adds a header only if a socket channel is used to send to a
	 * remote node, otherwise it hands over the message directly to the worker.
	 */
	protected int sendToIDInternal(NodeIDType destID, MessageType msg)
			throws IOException {
		if (destID.equals(this.myID))
			return sendLocal(msg);
		// else
		if (msg instanceof byte[])
			return this.stampHeaderAndSend(destID, (byte[]) msg);
		return this.stampHeaderAndSend(destID, msg.toString());
	}

	// bypass network send by directly passing to local worker
	private int sendLocal(MessageType message) {
		/*
		 * We create a deep copy for local sends as otherwise it can end up
		 * getting modified by the receiver end and cause the number of bytes
		 * written to be not equal to those sent for sends to receivers other
		 * than the sender when the same message is being sent to a set of nodes
		 * including self.
		 */

		String msg = message.toString();
		int length = msg.length();
		NIOInstrumenter.incrSent();
		((InterfaceMessageExtractor) worker)
				.processMessage(new InetSocketAddress(this.getNodeAddress(),
						this.getNodePort()), msg);
		return length;
	}

	protected static final String NIO_CHARSET_ENCODING = "ISO-8859-1";

	private int stampHeaderAndSend(Object id, String msg) throws IOException {
		int written = this.stampHeaderAndSendBytes(id, msg);
		return written < 0 ? written : msg.length();
	}

	/**
	 * 
	 * @param id
	 * @param msg
	 * @return Returns the number of characters written, not the number of
	 *         bytes. The number of characters written is either -1 if there was
	 *         an error or the length of the input string. It is not possible
	 *         for this method to return any other value as NIO transport either
	 *         accepts a complete message or none at all.
	 * @throws IOException
	 */
	private int stampHeaderAndSendBytes(Object id, String msg)
			throws IOException {
		String header = MessageExtractor.getHeader(msg);
		String headeredMsg = header + msg;
		int headerByteLength = headeredMsg.getBytes(NIO_CHARSET_ENCODING).length;
		byte[] headeredMsgBytes = headeredMsg.getBytes(NIO_CHARSET_ENCODING);
		// caller only passed NodeIDType or InetSocketAddress
		@SuppressWarnings("unchecked")
		int written = (id instanceof InetSocketAddress ? this.sendUnderlying(
				(InetSocketAddress) id, headeredMsgBytes) : this
				.sendUnderlying((NodeIDType) id, headeredMsgBytes))
				- headerByteLength; // subtract header length
		assert (written < 0 || written == (headeredMsgBytes.length - headerByteLength));
		// need to return the number of characters written
		return written;
	}

	/**
	 * We send even byte arrays encoded as strings because it is easier to do
	 * demultiplexing (in JSONMesageExtractor) working with strings than byte
	 * arrays. Using a fixed encoding only ensures that going from a string to
	 * bytes and back yields the same string. But we also want the property here
	 * that going from a byte array to a string and back yields the same byte
	 * array. This property is not ensured by all encodings but is ensured by
	 * ISO-8859-1, so we use that encoding.
	 */
	/**
	 * 
	 * @param id
	 * @param msgBytes
	 * @return The number of bytes written.
	 * @throws IOException
	 */
	private int stampHeaderAndSend(Object id, byte[] msgBytes)
			throws IOException {
		String msg = new String(msgBytes,
				MessageNIOTransport.NIO_CHARSET_ENCODING);
		return this.stampHeaderAndSendBytes(id, msg);
	}

	/**
	 * These methods are really redundant wrappers around the corresponding
	 * NIOTransport methods, but they exist so that there is one place where all
	 * NIO sends actually happen.
	 */
	private int sendUnderlying(NodeIDType id, byte[] data) throws IOException {
		return this.send(id, data);
	}

	private int sendUnderlying(InetSocketAddress isa, byte[] data)
			throws IOException {
		return this.send(isa, data);
	}

	// //////////////////////////////////////////////////////////////////////
	// for testing only
	private static JSONObject JSONify(int msgNum, String s)
			throws JSONException {
		return new JSONObject("{\"msg\" : \"" + s + "\" , \"msgNum\" : "
				+ msgNum + "}");
	}

	/*
	 * The test code here is mostly identical to that of NIOTransport but tests
	 * JSON messages, headers, and delay emulation features. Need to test it
	 * with the rest of GNS.
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		int msgNum = 0;
		int port = 2000;
		int nNodes = 100;
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(port);
		snc.localSetup(nNodes + 2);
		MessageExtractor[] workers = new MessageExtractor[nNodes + 1];
		for (int i = 0; i < nNodes + 1; i++) {
			workers[i] = new MessageExtractor(new PacketDemultiplexerDefault());
		}
		MessageNIOTransport<?, ?>[] niots = new MessageNIOTransport[nNodes];

		try {
			int smallNNodes = 2;
			for (int i = 0; i < smallNNodes; i++) {
				niots[i] = new MessageNIOTransport<Integer, JSONObject>(i, snc,
						workers[i]);
				new Thread(niots[i]).start();
			}

			/*
			 * Test a few simple hellos. The sleep is there to test that the
			 * successive writes do not "accidentally" benefit from concurrency,
			 * i.e., to check that OP_WRITE flags will be set correctly.
			 */
			((MessageNIOTransport<Integer, JSONObject>) niots[1])
					.sendToIDInternal(0, JSONify(msgNum++, "Hello from 1 to 0"));
			((MessageNIOTransport<Integer, JSONObject>) niots[0])
					.sendToIDInternal(1,
							JSONify(msgNum++, "Hello back from 0 to 1"));
			((MessageNIOTransport<Integer, JSONObject>) niots[0])
					.sendToIDInternal(1,
							JSONify(msgNum++, "Second hello back from 0 to 1"));
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			((MessageNIOTransport<Integer, JSONObject>) niots[0])
					.sendToIDInternal(1,
							JSONify(msgNum++, "Third hello back from 0 to 1"));
			((MessageNIOTransport<Integer, JSONObject>) niots[1])
					.sendToIDInternal(
							0,
							JSONify(msgNum++,
									"Thank you for all the hellos back from 1 to 0"));
			// //////////////////////////////////////////////////////////////////////
			int seqTestNum = 1;
			Thread.sleep(2000);
			System.out.println("\n\n\nBeginning test of " + seqTestNum
					+ " random, sequential messages");
			Thread.sleep(1000);

			// //////////////////////////////////////////////////////////////////////

			// Create the remaining nodes up to nNodes
			for (int i = smallNNodes; i < nNodes; i++) {
				niots[i] = new MessageNIOTransport<Integer, JSONObject>(i, snc,
						workers[i]);
				new Thread(niots[i]).start();
			}

			// Test a random, sequential communication pattern
			for (int i = 0; i < nNodes * seqTestNum; i++) {
				int k = (int) (Math.random() * nNodes);
				int j = (int) (Math.random() * nNodes);
				System.out.println("Message " + i + " with msgNum " + msgNum);
				((MessageNIOTransport<Integer, JSONObject>) niots[k])
						.sendToIDInternal(
								j,
								JSONify(msgNum++, "Hello from " + k + " to "
										+ j));
			}

			int oneToOneTestNum = 1;
			// //////////////////////////////////////////////////////////////////////

			Thread.sleep(1000);
			System.out
					.println("\n\n\nBeginning test of "
							+ oneToOneTestNum
							* nNodes
							+ " random, concurrent, 1-to-1 messages with emulated delays");
			Thread.sleep(1000);
			// //////////////////////////////////////////////////////////////////////

			// Random, concurrent communication pattern with emulated delays
			ScheduledExecutorService execpool = Executors
					.newScheduledThreadPool(5);
			class TX extends TimerTask {

				MessageNIOTransport<Integer, JSONObject> sndr = null;
				private int rcvr = -1;
				int msgNum = -1;

				TX(int i, int id, MessageNIOTransport<?, ?>[] n, int m) {
					sndr = (MessageNIOTransport<Integer, JSONObject>) n[i];
					rcvr = id;
					msgNum = m;
				}

				TX(MessageNIOTransport<Integer, JSONObject> niot, int id, int m) {
					sndr = niot;
					rcvr = id;
					msgNum = m;
				}

				public void run() {
					try {
						sndr.sendToIDInternal(
								rcvr,
								JSONify(msgNum, "Hello from " + sndr.myID
										+ " to " + rcvr));
					} catch (IOException e) {
						e.printStackTrace();
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			JSONDelayEmulator.emulateDelays();

			MessageNIOTransport<Integer, JSONObject> concurrentSender = new MessageNIOTransport<Integer, JSONObject>(
					nNodes, snc, workers[nNodes]);
			new Thread(concurrentSender).start();
			ScheduledFuture<?>[] futuresRandom = new ScheduledFuture[nNodes
					* oneToOneTestNum];
			for (int i = 0; i < nNodes * oneToOneTestNum; i++) {
				TX task = new TX(concurrentSender, 0, msgNum++);
				System.out.println("Scheduling random message " + i
						+ " with msgNum " + msgNum);
				futuresRandom[i] = execpool.schedule(task, 0,
						TimeUnit.MILLISECONDS);
			}
			for (int i = 0; i < nNodes * oneToOneTestNum; i++) {
				try {
					futuresRandom[i].get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// //////////////////////////////////////////////////////////////////////
			Thread.sleep(1000);
			System.out.println("\n\n\nBeginning test of random, concurrent, "
					+ " any-to-any messages with emulated delays");
			Thread.sleep(1000);
			// //////////////////////////////////////////////////////////////////////

			int load = nNodes * 25;
			int msgsToFailed = 0;
			ScheduledFuture<?>[] futures = new ScheduledFuture[load];
			for (int i = 0; i < load; i++) {
				int k = (int) (Math.random() * nNodes);
				int j = (int) (Math.random() * nNodes);
				// long millis = (long)(Math.random()*1000);

				if (i % 100 == 0) {
					// Periodically try sending to a non-existent node
					j = nNodes + 1;
					msgsToFailed++;
				}

				TX task = new TX(k, j, niots, msgNum++);
				System.out.println("Scheduling random message " + i
						+ " with msgNum " + msgNum);
				futures[i] = (ScheduledFuture<?>) execpool.schedule(task, 0,
						TimeUnit.MILLISECONDS);
			}
			int numExceptions = 0;
			for (int i = 0; i < load; i++) {
				try {
					futures[i].get();
				} catch (Exception e) {
					// e.printStackTrace();
					numExceptions++;
				}
			}

			// ////////////////////////////////////////////////////////////////
			Thread.sleep(2000);
			System.out
					.println("\n\n\nPrinting overall stats. Number of exceptions =  "
							+ numExceptions);
			System.out.println((new NIOInstrumenter() + "\n"));
			boolean pending = false;
			for (int i = 0; i < nNodes; i++) {
				if (niots[i].getPendingSize() > 0) {
					System.out.println("Pending messages at node " + i + " : "
							+ niots[i].getPendingSize());
					pending = true;
				}
			}
			int missing = NIOInstrumenter.getMissing();
			assert (pending == false || missing == msgsToFailed) : "Unsent pending messages in NIO";
			for (NIOTransport<?> niot : niots) {
				niot.stop();
			}
			concurrentSender.stop();
			execpool.shutdown();

			if (!pending || missing == msgsToFailed) {
				System.out
						.println("\nSUCCESS: no pending messages to non-failed nodes!");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
