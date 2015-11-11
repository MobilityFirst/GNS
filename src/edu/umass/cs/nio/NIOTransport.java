/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.nio;

import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.DataProcessingWorker;
import edu.umass.cs.nio.interfaces.HandshakeCallback;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.DataProcessingWorkerDefault;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.utils.Stringer;
import edu.umass.cs.utils.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is designed for NIO transport. The nice thing about
 *            this class is that all steps are truly asynchronous. Even connect
 *            calls. There is no blocking on connect, read, or write anywhere
 *            and no polling in the code.
 * 
 *            The code can be used to send a byte stream to a numbered node
 *            (with ID->InetSocketAddress mappings specified using the
 *            NodeConfig interface) or to directly send messages to an
 *            InetSocketAddress. Received data is processed using the
 *            DataProcessingWorker interface. Received data can not be
 *            associated with a node ID as the received data just looks like a
 *            byte stream to the receiving node. The supplied
 *            DataProcessingWorker is expected to know what to do with the
 *            received byte streams.
 * 
 *            The high-level organization of this code has a selector thread
 *            that waits for connect, accept, read, or write events on socket
 *            channels that could result from accepting connections (server
 *            side) or from initiating connections upon send requests by
 *            application threads. The selector thread also writes to socket
 *            channels and reads incoming data from socket channels and passes
 *            them off to DataProcessingWorker. The selector thread is the only
 *            thread to touch socket channel selection keys, e.g., to change ops
 *            or select() on them. Other application threads may invoke send()
 *            concurrently to send data. This data is queued in pendingWrites, a
 *            synchronized structure between application threads and the
 *            selector thread. A pending write is associated with an
 *            InetSocketAddress. So, the selector thread can always re-establish
 *            a connection to continue writing the byte stream in case the
 *            existing connection fails. The selector thread reads from
 *            pendingWrites and sets selection ops to wait for write-ready
 *            events accordingly.
 * 
 *            To enable non-blocking connects, the application threads queue
 *            connect events in the synchronized structure pendingConnects. The
 *            selector thread reads from pendingConnects and sets selection ops
 *            to wait for a connect event as needed.
 * 
 *            A map SockAddrToSockChannel keeps track of the current socket
 *            channel being used to send data to a given InetSocketAddress. Note
 *            that this mapping can change if connections fail and are
 *            re-established. A failed connection can get re-established on
 *            demand by an application thread or by the selector thread when it
 *            tries to actually write the data to a socket channel and
 *            encounters an exception.
 */
public class NIOTransport<NodeIDType> implements Runnable,
		HandshakeCallback {

	/**
	 * Number of sends that can be queued because the connection was established
	 * but the remote end crashed before the send was complete. Note that this
	 * is the number of packets, not bytes. Currently, there is no way to set a
	 * byte limit for the queue size.
	 */
	public static final int MAX_QUEUED_SENDS = 1024*128;
	private int maxQueuedSends = MAX_QUEUED_SENDS;

	/**
	 * @param maxQ
	 *            Refer {@link #MAX_QUEUED_SENDS MAX_QUEUED_SENDS}.
	 */
	public void setMaxQueuedSends(int maxQ) {
		this.maxQueuedSends = maxQ;
	}

	private int getMaxQueuedSends() {
		return this.maxQueuedSends;
	}

	/**
	 * Milliseconds before reconnection attempts.
	 */
	public static final int MIN_INTER_CONNECT_TIME = 5000;
	private int minInterConnectTime = MIN_INTER_CONNECT_TIME;

	/**
	 * @param minInterConnectTime
	 *            Refer {@link #MIN_INTER_CONNECT_TIME MIN_INTER_CONNECT_TIME}.
	 */
	public void setMinInterConnectTime(int minInterConnectTime) {
		this.minInterConnectTime = minInterConnectTime;
	}

	private int getMinInterConnectTime() {
		return this.minInterConnectTime;
	}

	/**
	 * Hint to set socket buffer size (that may be ignored by the system).
	 */
	private static final int HINT_SOCK_BUFFER_SIZE = 512000;

	/**
	 * True means duplex, so it will work even with one end behind a NAT.
	 */
	private static final boolean DUPLEX_CONNECTIONS = true;

	/**
	 * Max size we read per read off of the socket. This will limit NIO
	 * throughput if typical messages are much bigger than this limit.
	 */
	private static final int READ_BUFFER_SIZE = 1024 * 64;
	private static final int WRITE_BUFFER_SIZE = 1024 * 256;

	/**
	 * Usually an id that corresponds to a socket address as specified in
	 * NodeConfig. Note that myID can also <code>null</code> be which means
	 * wildcard address or it can be a InetSocket address in the case of Local
	 * Name Servers
	 */
	protected final NodeIDType myID;

	// data processing worker to hand off received messages
	protected final DataProcessingWorker worker;

	// Maps id to socket address
	protected final NodeConfig<NodeIDType> nodeConfig;

	// selector we'll be monitoring
	private Selector selector = null;

	private final ByteBuffer writeBuffer = ByteBuffer
			.allocateDirect(WRITE_BUFFER_SIZE);

	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// List of pending connects on which finishConnect needs to be called.
	private LinkedList<ChangeRequest> pendingConnects = null;

	/*
	 * The key is a socket address and the value is a list of messages to be
	 * sent to that socket address.
	 */
	private ConcurrentHashMap<InetSocketAddress, LinkedBlockingQueue<ByteBuffer>> sendQueues = null;

	/*
	 * Maps a socket address to a socket channel. The latter may change in case
	 * a connection breaks and a new one needs to be initiated.
	 */
	private HashMap<InetSocketAddress, SocketChannel> sockAddrToSockChannel = null;

	/* Map to optimize connection attempts by the selector thread. */
	private ConcurrentHashMap<InetSocketAddress, Long> connAttempts = null;
	
	private ConcurrentHashMap<NodeIDType,Long> lastFailed = null;
	
	private SenderTask senderTask;

	private boolean started = false;

	private boolean stopped = false;

	/**
	 * A flag to easily enable or disable SSL by default. CLEAR is not really a
	 * legitimate SSL mode, i.e., it is not supported by
	 * SSLDataProcessingWorker, but we have it here for ease of changing the
	 * default.
	 */
	protected static final SSLDataProcessingWorker.SSL_MODES DEFAULT_SSL_MODE = SSLDataProcessingWorker.SSL_MODES.CLEAR;

	private static final Logger log = Logger.getLogger(NIOTransport.class
			.getName());

	/**
	 * @return java.util.logging.Logger used by NIOTransport.
	 */
	public static Logger getLogger() {
		return log;
	}

	// private constructor must remain private
	private NIOTransport(NodeIDType id, NodeConfig<NodeIDType> nc,
			InetSocketAddress mySockAddr, DataProcessingWorker worker,
			boolean start, SSLDataProcessingWorker.SSL_MODES sslMode)
			throws IOException {
		this.myID = id;
		// null node config means no ID-based communication possible
		this.nodeConfig = nc;

		this.worker = this.getWorker(worker, sslMode);

		// non-final fields, but they never change after constructor anyway
		this.selector = this.initSelector(mySockAddr);
		this.pendingConnects = new LinkedList<ChangeRequest>();
		this.sendQueues = new ConcurrentHashMap<InetSocketAddress, LinkedBlockingQueue<ByteBuffer>>();
		this.sockAddrToSockChannel = new HashMap<InetSocketAddress, SocketChannel>();
		this.connAttempts = new ConcurrentHashMap<InetSocketAddress, Long>();
		this.lastFailed = new ConcurrentHashMap<NodeIDType, Long>();

		if (start) {
			Thread me = (new Thread(this));
			me.setName(getClass().getSimpleName() + myID);
			me.start();
			
			this.senderTask = new SenderTask();
			this.senderTask.setName(getClass().getSimpleName()
					+ SenderTask.class.getSimpleName() + myID);
			if (useSenderTask())
				this.senderTask.start();
		}
	}

	/**
	 * The constructor to use for ID-based communication.
	 * 
	 * @param id
	 * @param nc
	 * @param worker
	 * @throws IOException
	 */
	public NIOTransport(NodeIDType id, NodeConfig<NodeIDType> nc,
			DataProcessingWorker worker) throws IOException {
		this(id, nc, (id == null ? new InetSocketAddress(0) : null), worker,
				true, DEFAULT_SSL_MODE);
	}

	/**
	 * @param id
	 * @param nc
	 * @param worker
	 * @param start
	 * @param sslMode
	 * @throws IOException
	 */
	public NIOTransport(NodeIDType id, NodeConfig<NodeIDType> nc,
			DataProcessingWorker worker, boolean start,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		this(id, nc, (id == null ? new InetSocketAddress(0) : null), worker,
				true, sslMode);
	}

	/**
	 * @param id
	 * @param nc
	 * @param worker
	 * @param sslMode
	 * @throws IOException
	 */
	public NIOTransport(NodeIDType id, NodeConfig<NodeIDType> nc,
			DataProcessingWorker worker,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		this(id, nc, (id == null ? new InetSocketAddress(0) : null), worker,
				true, sslMode);
	}

	/**
	 * @param port
	 * @param worker
	 * @throws IOException
	 */
	public NIOTransport(int port, DataProcessingWorker worker)
			throws IOException {
		this(null, null, new InetSocketAddress(port), worker, true,
				DEFAULT_SSL_MODE);
	}

	/**
	 * @param address
	 * @param port
	 * @param worker
	 * @param sslMode
	 * @throws IOException
	 */
	public NIOTransport(InetAddress address, int port,
			DataProcessingWorker worker,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		this(null, null, new InetSocketAddress(address, port), worker, true,
				sslMode);
	}

	protected NIOTransport(NIOTransport<NodeIDType> niot) {
		this.myID = niot.myID;
		this.nodeConfig = niot.nodeConfig;
		this.worker = niot.worker;
	}

	private DataProcessingWorker getWorker(
			DataProcessingWorker worker,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		try {
			if (sslMode.equals(SSLDataProcessingWorker.SSL_MODES.SERVER_AUTH)
					|| sslMode
							.equals(SSLDataProcessingWorker.SSL_MODES.MUTUAL_AUTH)) {
				return (worker instanceof SSLDataProcessingWorker ? (SSLDataProcessingWorker) worker
						: new SSLDataProcessingWorker(worker, sslMode))
						.setHandshakeCallback(this);
				// CLEAR is not a legitimate SSLDataProcessingWorker type
			}
		} catch (NoSuchAlgorithmException e) {
			throw new IOException(e.getMessage());
		}
		return worker;
	}

	/**
	 * send() methods are called by external application threads. They may
	 * initiate a connection if one is not available. However, the connection is
	 * finished in a non-blocking manner by the selector thread. Data to be sent
	 * is queued in pendingWrites, which is read later by the selector thread.
	 * 
	 * @param id
	 * @param data
	 * @return Number of bytes sent.
	 * @throws IOException
	 */
	public int send(NodeIDType id, byte[] data) throws IOException {
		log.log(Level.FINEST,
				"{0} invoked send to ({1}={2}:{3}: {4}), checking connection status..",
				new Object[] { this, id, this.nodeConfig.getNodeAddress(id),
						this.nodeConfig.getNodePort(id), new Stringer(data) });
		if (this.nodeConfig == null)
			throw new NullPointerException(
					"Attempting ID-based communication with null InterfaceNodeConfig");
		return send(new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)), data);
	}

	/**
	 * @param isa
	 * @param data
	 * @return Number of bytes sent.
	 * @throws IOException
	 */
	public int send(InetSocketAddress isa, byte[] data) throws IOException {
		if(isa==null) return -1;
		if (data.length > MAX_PAYLOAD_SIZE)
			throw new IOException("Packet size of " + data.length
					+ " exceeds maximum allowed payload size of "
					+ MAX_PAYLOAD_SIZE);		
		
		testAndIntiateConnection(isa);
		// we put length header in *all* messages
		ByteBuffer bbuf = getHeaderedByteBuffer(data=this.deflate(data));
		int written = this.canEnqueueSend(isa) ? this.enqueueSend(isa, bbuf) : 0;
		return written > 0 ? written - HEADER_SIZE : written;
	}

	private byte[] deflate(byte[] data) {
		if(isSSL() || !getCompression() || data.length < getCompressionThreshold()) return data;

		Deflater deflator = new Deflater();
		byte[] compressed = new byte[data.length];
		int compressedLength = data.length;
		deflator.setInput(data);
		deflator.finish();
		compressedLength = deflator.deflate(compressed);
		deflator.end();
		data = new byte[compressedLength];
		for(int i=0; i<data.length; i++) data[i] = compressed[i];
		return data;
	}
	

	/**
	 * For performance testing. Repeats {@code data} {@code batchSize} number of
	 * times in order to simulate the performance of batched sends.
	 * 
	 * @param isa
	 * @param data
	 * @param batchSize
	 * @return Number of bytes written.
	 * @throws IOException
	 */
	public int send(InetSocketAddress isa, byte[] data, int batchSize)
			throws IOException {
		testAndIntiateConnection(isa);
		ByteBuffer bbuf = ByteBuffer.allocate((HEADER_SIZE + data.length)
				* batchSize);
		for (int i = 0; i < batchSize; i++)
			putHeaderLength(bbuf, data.length).put(data);
		bbuf.flip();
		int written = this.canEnqueueSend(isa) ? this.enqueueSend(isa, bbuf) : 0;
		return written > 0 ? written - batchSize * HEADER_SIZE : written;
	}

	private static ByteBuffer getHeaderedByteBuffer(byte[] data) {
		ByteBuffer bbuf = ByteBuffer.allocate(HEADER_SIZE + data.length);
		putHeaderLength(bbuf, data.length).put(data);
		assert (!bbuf.hasRemaining() && bbuf.capacity() == (HEADER_SIZE + data.length));
		bbuf.flip();
		return bbuf;
	}

	private static ByteBuffer putHeaderLength(ByteBuffer buf, int length) {
		return (USE_PREAMBLE ? buf.putInt(PREAMBLE) : buf).putInt(length);
	}

	protected static int getPayloadLength(ByteBuffer buf) throws IOException {
		assert (buf.capacity() == HEADER_SIZE);
		int preamble = -1;
		if (!USE_PREAMBLE || (preamble = buf.getInt()) == PREAMBLE) {
			int length = buf.getInt();
			if (outOfRange(length))
				throw new IOException("Out-of-range payload length " + length);
			return length;
		}
		// error: empty out buf
		byte[] b = new byte[buf.remaining()];
		buf.get(b);
		throw new IOException(" Parsed bad preamble " + preamble + " before: ["
				+ new String(b) + "]");
	}

	private static int PREAMBLE = 723432553;
	// not really needed; could be true or false
	private static boolean USE_PREAMBLE = true;
	protected static int HEADER_SIZE = USE_PREAMBLE ? 8 : 4;

	private static final long SELECT_TIMEOUT = 2000;

	public void run() {
		// to not double-start, but check not thread-safe
		if (this.started)
			return;
		this.started = true;
		while (!isStopped()) {
			try {
				/*
				 * Set ops to WRITE for pending write requests. We don't need to
				 * do this every select iteration, just once in a few
				 * iterations. We need to do it at all so that the selector
				 * thread can retry connecting to not-yet-connected destinations
				 * with pending writes.
				 */
				registerWriteInterests();
				// set ops to CONNECT for pending connect requests.
				processPendingConnects();
				// wait for an event one of the registered channels.
				this.selector.select(SELECT_TIMEOUT);
				// accept, connect, read, or write as needed.
				processSelectedKeys();
				// process data from pending buffers on congested channels
				this.tryProcessCongested();
			} catch (Exception e) {
				/*
				 * Can do little else here. Hopefully, the exceptions inside the
				 * individual methods above have already been contained.
				 */
				log.severe(this + " incurred IOException " + e.getMessage());
				e.printStackTrace();
			}
		}
		try {
			this.senderTask.close();
			this.selector.close();
			this.serverChannel.close();
			if (this.worker instanceof SSLDataProcessingWorker)
				((SSLDataProcessingWorker) this.worker).stop();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	protected boolean isStarted() {
		return this.started;
	}

	/**
	 * To close NIOTransport instances gracefully.
	 */
	public synchronized void stop() {
		this.stopped = true;
		this.senderTask.close();
		this.selector.wakeup();
	}

	protected synchronized boolean isStopped() {
		return this.stopped;
	}

	protected InetAddress getNodeAddress() {
		if (this.myID == null) {
			return this.getListeningAddress();
		} else {
			return this.nodeConfig.getNodeAddress(myID);
		}
	}

	protected int getNodePort() {
		if (this.myID == null) {
			return this.getListeningPort();
		} else {
			return this.nodeConfig.getNodePort(myID);
		}
	}
	
	/**
	 * @param node
	 * @return Whether {@code node} got disconnected.
	 */
	public boolean isDisconnected(NodeIDType node) {
		return this.lastFailed.containsKey(node);
	}

	/* ********** Start of private methods ************************** */

	private boolean isDisconnected(InetSocketAddress isa) {
		NodeIDType node = this.getNodeID(isa);
		return node!=null ? this.lastFailed.containsKey(node) : false;
	}


	// Invoked only by the selector thread. Typical nio event handling code.
	private void processSelectedKeys() {
		// Iterate over the set of keys for which events are available
		ArrayList<SelectionKey> selected = new ArrayList<SelectionKey>(
				this.selector.selectedKeys());
		Iterator<SelectionKey> selectedKeys = selected.iterator();
		Collections.shuffle(selected); // to mix in reads and writes

		while (selectedKeys.hasNext()) {
			SelectionKey key = (SelectionKey) selectedKeys.next();
			selectedKeys.remove();

			if (!key.isValid()) {
				cleanup(key, (AbstractSelectableChannel) key.channel());
				continue;
			}
			try {
				// Check what event is available and deal with it
				if (key.isValid() && key.isAcceptable())
					this.accept(key);
				if (key.isValid() && key.isConnectable())
					this.finishConnection(key);
				if (key.isValid() && key.isWritable()) 
					if (useSenderTask())
						this.senderTask.addKey(key);
					else
						this.write(key);
				if (key.isValid() && key.isReadable()) 
					this.read(key);
			} catch (IOException | CancelledKeyException e) {
				updateFailed(key);
				log.info("Node" + myID
						+ " incurred IOException on "
						+ key.channel()
						+ " likely because remote end closed connection");
				cleanup(key, (AbstractSelectableChannel) key.channel());
				e.printStackTrace(); // print and move on with other keys
			}
		}
		this.selector.selectedKeys().clear();
	}
	
	private void updateFailed(SelectionKey key) {
		if(!(key.channel() instanceof SocketChannel)) return;
		SocketChannel channel = (SocketChannel) key.channel();
		InetSocketAddress remote = (InetSocketAddress) channel.socket()
				.getRemoteSocketAddress();
		if (remote == null)
			remote = this.getSockAddrFromSockChannel(channel);
		this.updateFailed(remote);
	}
	private void updateFailed(InetSocketAddress remote) {
		NodeIDType node = null;
		if ((node = this.getNodeID(remote)) != null)
			this.lastFailed.put(node, System.currentTimeMillis());
	}
	
	private NodeIDType getNodeID(InetSocketAddress isa) {
		if (isa != null && this.nodeConfig != null)
			for (NodeIDType node : this.nodeConfig.getNodeIDs())
				if (this.nodeConfig.getNodeAddress(node).equals(
						isa.getAddress())
						&& this.nodeConfig.getNodePort(node) == isa.getPort())
					return node;
		return null;
	}

	private void updateAlive(SocketChannel channel) {
		InetSocketAddress remote = (InetSocketAddress) (channel).socket()
				.getRemoteSocketAddress();
		if (remote == null)
			remote = this.getSockAddrFromSockChannel(channel);
		NodeIDType node = null;
		if ((node = this.getNodeID(remote)) != null)
			this.lastFailed.remove(node);
	}

	/*
	 * Invoked only by the selector thread. accept immediately sets the channel
	 * in read mode as a connection is presumably being established to send some
	 * data. We could also insert a mapping into SockAddrToSockChannel, but this
	 * is unlikely to be useful as responses will be addressed to the
	 * destination's InetSocketAddress, which would not be the same as the
	 * remote address of the socket channel just established.
	 */
	private void accept(SelectionKey key) throws IOException {

		// For an accept to be pending the channel must be a server socket
		// channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
				.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		if (socketChannel == null)
			return;
		log.log(Level.FINE, "{0} accepted connection from {1}", new Object[] {
				this, socketChannel.getRemoteAddress() });
		NIOInstrumenter.incrAccepted();
		socketChannel.socket().setKeepAlive(true);
		socketChannel.configureBlocking(false);
		socketChannel.socket().setTcpNoDelay(true);
		socketChannel.socket().setReceiveBufferSize(HINT_SOCK_BUFFER_SIZE);
		socketChannel.socket().setSendBufferSize(HINT_SOCK_BUFFER_SIZE);
		
		this.updateAlive(socketChannel);

		/*
		 * Register the new SocketChannel with our Selector, indicating we'd
		 * like to be notified when there's data waiting to be read. We could
		 * have also use key.selector() below.
		 */
		SelectionKey socketChannelKey = socketChannel.register(this.selector,
				SelectionKey.OP_READ);

		// Try to reuse accepted connection for sending data
		if (DUPLEX_CONNECTIONS)
			this.reuseAcceptedConnectionForWrites(socketChannel);
		socketChannelKey.attach(new AlternatingByteBuffer()); // for length
		assert (socketChannelKey.attachment() != null);

		registerSSL(socketChannelKey, false);
	}

	/**
	 * Invoked only by the selector thread. read() is easy as it just needs to
	 * read whatever is available and send it off to DataProcessingWorker (that
	 * has to deal with the complexity of parsing a byte stream).
	 */
	public static final int MAX_PAYLOAD_SIZE = 4 * 1024 * 1024;

	// used also by SSLDataProcessingWorker
	protected static class AlternatingByteBuffer {
		final ByteBuffer headerBuf;
		ByteBuffer bodyBuf = null;

		AlternatingByteBuffer() {
			headerBuf = ByteBuffer.allocate(HEADER_SIZE);
			bodyBuf = null;
		}

		void clear() {
			this.headerBuf.clear();
			this.bodyBuf = null;
		}
	}

	private final ConcurrentHashMap<SelectionKey, ByteBuffer> readBuffers = new ConcurrentHashMap<SelectionKey, ByteBuffer>();

	private final ConcurrentHashMap<SelectionKey, AlternatingByteBuffer> congested = new ConcurrentHashMap<SelectionKey, AlternatingByteBuffer>();

	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		// if SSL, simply pass any bytes to SSL worker
		if (isSSL()) {
			this.readBuffers.putIfAbsent(key,
					ByteBuffer.allocate(READ_BUFFER_SIZE));
			ByteBuffer bbuf = this.readBuffers.get(key); // this.readBuffer;
			int numRead = socketChannel.read(bbuf);
			if (numRead > 0) {
				bbuf.flip();
				try {
					this.worker.processData(socketChannel, bbuf);
					assert (bbuf.remaining() == 0);
				} catch (BufferOverflowException boe) {
					// do nothing, bbuf will read more later
				}
				bbuf.compact();
			}
			return;
		}
		// else first read payload length in header, then ready payload
		AlternatingByteBuffer abbuf = (AlternatingByteBuffer) key
				.attachment();
		assert (abbuf != null) : this + ": no attachment for " + key.channel();

		// read into body if header completely read, else read into header
		ByteBuffer bbuf = (abbuf.headerBuf.remaining() == 0 ? abbuf.bodyBuf
				: abbuf.headerBuf);
		// end-of-stream => cleanup
		if (bbuf.hasRemaining() && socketChannel.read(bbuf) < 0) {
			cleanup(key);
			return;
		}

		// parse header for length if complete header read
		if (bbuf == abbuf.headerBuf && !bbuf.hasRemaining()) {
			bbuf.flip();
			int length = -1;
			try {
				length = getPayloadLength(bbuf);
			} catch (IOException ioe) {
				throw new IOException("Node" + myID + ":"+ioe.getMessage()
						+ " on channel " + socketChannel);
			}
			// allocate new buffer and read payload
			bbuf = (abbuf.bodyBuf = ByteBuffer.allocate(length));
			socketChannel.read(bbuf);
		}

		// if complete payload read, pass to worker
		if (abbuf.bodyBuf != null && !abbuf.bodyBuf.hasRemaining()) {
			bbuf.flip();
			this.worker.processData(socketChannel, this.inflate(bbuf));
			// clear header to prepare to read the next message
			if (!bbuf.hasRemaining()) {
				abbuf.clear();
				this.congested.remove(key);
			}
			// else worker has not finished reading
			else {
				bbuf.compact();
				assert (!bbuf.hasRemaining()); // all or nothing processing
				// check later to prevent the last one from hanging
				congested.putIfAbsent(key, abbuf);
			}
		}

		/*
		 * When we hand the data off to our worker thread, the worker is
		 * expected to synchronously get() everything from readBuffer and 
		 * return, i.e., no partial reads, the flip/compact structure
		 * above notwithstanding.
		 */
	}
	
	private void tryProcessCongested() throws IOException {
		for (Iterator<SelectionKey> keyIter = this.congested.keySet()
				.iterator(); keyIter.hasNext();) {
			SelectionKey key = keyIter.next();
			if (key.isValid())
				this.read(key);
			else
				keyIter.remove();
		}
	}
	
	private static boolean enableCompression = true;
	/**
	 * @param b
	 */
	public static void setCompression(boolean b) {
		enableCompression = b;
	}
	/**
	 * @return True if compression enabled.
	 */
	public static boolean getCompression() {
		return enableCompression && compressionThreshold < MAX_PAYLOAD_SIZE;
	}
	
	// default effectively disables compression
	private static int compressionThreshold = MAX_PAYLOAD_SIZE;

	/**
	 * @param t
	 */
	public static void setCompressionThreshold(int t) {
		// FIXME: compression is disabled
		//compressionThreshold = t;
	}

	/**
	 * @return Compression threshold
	 */
	public static int getCompressionThreshold() {
		return compressionThreshold;
	}
	
	private ByteBuffer inflate(ByteBuffer bbuf) throws IOException {
		if(isSSL() || !getCompression()) return bbuf;
		
		Inflater inflator = new Inflater();
		inflator.setInput(bbuf.array(), 0, bbuf.capacity());
		byte[] decompressed = new byte[bbuf.capacity()];
		ByteArrayOutputStream baos = new ByteArrayOutputStream(bbuf.capacity());
		try {
			while (!inflator.finished()) {
				int count = inflator.inflate(decompressed);
				if (count == 0)
					break;
				baos.write(decompressed, 0, count);
			}
			baos.close();
			inflator.end();
		} catch (DataFormatException e) {
			// possible for exception to be legitimate even if below threshold
			if(bbuf.capacity() > getCompressionThreshold()) {
				log.severe(this + " incurred DataFormatException ");
				e.printStackTrace();
			} 
			return bbuf;
		}
		bbuf = ByteBuffer.wrap(baos.toByteArray());
		return bbuf;
	}

	/*
	 * Invoked only by the selector thread. If a write encounters an exception,
	 * the selector thread may establish a new connection.
	 */
	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		try {
			InetSocketAddress isa = (InetSocketAddress) socketChannel
					.getRemoteAddress();
			// getSockAddrFromSockChannel(socketChannel);
			if (isa == null) { // should never happen
				log.severe("Null socket address for a write-ready socket!");
				cleanup(key, socketChannel);
			} else {
				// If all data written successfully, switch back to read mode.
				if (this.writeAllPendingWrites(isa, socketChannel))
					key.interestOps(SelectionKey.OP_READ);
			}
		} catch (IOException e) {
			// close socket channel and retry (but may still fail)
			this.cleanupRetry(key, socketChannel,
					this.getSockAddrFromSockChannel(socketChannel));
		}
	}
	
	// will clear both pending writes and
	private void clearPending(SocketChannel socketChannel) {
		InetSocketAddress sockAddr = this
				.getSockAddrFromSockChannel(socketChannel);
		/* Invariant: if there is data buffered to a destination, we must have
		 * a socket channel for it.
		 */
		synchronized (this.sendQueues) {
			this.sendQueues.remove(sockAddr);
			synchronized (this.sockAddrToSockChannel) {
				this.sockAddrToSockChannel.remove(sockAddr);
			}
		}
	}
	
	private static boolean useSenderTask = true;
	private static final boolean useSenderTask() {
		return useSenderTask;
	}
	/**
	 * @param b 
	 * 
	 */
	public static final void setUseSenderTask(boolean b) {
		useSenderTask = b;
	}
	class SenderTask extends Thread {
		LinkedBlockingQueue<SelectionKey> selectedKeys = new LinkedBlockingQueue<SelectionKey>();
		private boolean stopped = false;
		private static final long PO_TIMEOUT = 1000;

		public void close() {
			this.stopped = true;
		}

		void addKey(SelectionKey key) {
			assert(key!=null);
			if (!this.selectedKeys.contains(key)) {
				try {
					this.selectedKeys.offer(key, PO_TIMEOUT, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		SelectionKey pluckHead() {
			try {
				return this.selectedKeys.poll(PO_TIMEOUT, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public void run() {
			SelectionKey key = null;
			while (!stopped) {
				if ((key = this.pluckHead()) != null && key.isValid()
						&& key.isWritable()) {
					try {
						NIOTransport.this.write(key);
					} catch (IOException e) {
						NIOTransport.this.updateFailed(key);
						log.info("Node"
								+ myID
								+ " incurred IOException on "
								+ key.channel()
								+ " likely because remote end closed connection");
						cleanup(key);
						e.printStackTrace();
					}
				}
			}
		}
	}

	/* Start of methods synchronizing on pendingWrites. */

	/*
	 * Invoked only by the selector thread. Writes to socket channel and
	 * de-queues writes. If an exception occurs, it lets the bytes remain in the
	 * queue. However, we don't know if or how many of those bytes got sent to
	 * the other side. Basically, socket exceptions are bad news and will likely
	 * result in some missing data.
	 */
	private boolean writeAllPendingWrites(InetSocketAddress isa,
			SocketChannel socketChannel) throws IOException {
		LinkedBlockingQueue<ByteBuffer> sendQueue = this.sendQueues.get(isa);
		// possible if queuePendingWrite has not yet happened after connect
		if (sendQueue == null)
			return true;

		if (SEND_BATCHED)
			this.sendBatched(sendQueue, socketChannel);
		else
			this.sendUnbatched(sendQueue, socketChannel);

		if (sendQueue.isEmpty()) // check before locking
			this.dequeueSendQueueIfEmpty(isa, sendQueue);

		// caller will switch back to read mode if empty
		return (sendQueue.isEmpty());
	}

	private static boolean SEND_BATCHED = true; // default true

	// dequeue and send one message at a time
	private void sendUnbatched(LinkedBlockingQueue<ByteBuffer> sendQueue,
			SocketChannel socketChannel) throws IOException {
		while (!sendQueue.isEmpty()) {
			ByteBuffer buf0 = (ByteBuffer) sendQueue.peek();
			this.wrapWrite(socketChannel, buf0); // hook to SSL here
			// if socket's buffer fills up, let the rest be in queue
			log.log(Level.FINEST, "{0} wrote \"{1}\" to {2}",
					new Object[] { this, new Stringer(buf0.array()),
							socketChannel.getRemoteAddress() });
			if (buf0.remaining() > 0) {
				log.fine(this
						+ " socket buffer congested because of high load..");
				break;
			}
			assert (buf0.remaining() == 0);
			sendQueue.remove(); // remove buf0
		}
	}

	// use a large bytebuffer to batch and send
	private void sendBatched(LinkedBlockingQueue<ByteBuffer> sendQueue,
			SocketChannel socketChannel) throws IOException {
		// copy as much as possible into writeBuffer
		this.writeBuffer.clear();
		for (ByteBuffer buf : sendQueue) {
			if (writeBuffer.remaining() < buf.remaining())
				// cut out exactly as much as writeBuffer can accommodate
				buf = (ByteBuffer) buf.slice().limit(writeBuffer.remaining());

			int prevPos = buf.position();
			writeBuffer.put(buf);
			buf.position(prevPos);
			if (writeBuffer.remaining() == 0)
				break;
		}

		// flip and send out
		this.writeBuffer.flip();
		this.writeBuffer.remaining();
		int written = this.wrapWrite(socketChannel, this.writeBuffer);
		// assert(this.writeBuffer.remaining()==0);
		log.log(Level.FINEST,
				"{0} wrote {1} batched bytes to {2}; total sentByteCount = {3}",
				new Object[] { this, written, socketChannel.getRemoteAddress() });

		// remove exactly what got sent above
		while (!sendQueue.isEmpty()) {
			ByteBuffer buf = sendQueue.peek();
			int partial = buf.remaining() - written;
			if (partial > 0) {
				// buf didn't get fully sent
				buf.position(buf.position() + written);
				break;
			}
			// remove buf coz it got fully sent
			written -= buf.remaining();
			sendQueue.remove();
		}
	}

	private void dequeueSendQueueIfEmpty(InetSocketAddress isa,
			LinkedBlockingQueue<ByteBuffer> sendQueue) {
		synchronized (this.sendQueues) {
			// synchronized queue -> pendingWrites
			if (sendQueue.isEmpty())
				this.sendQueues.remove(isa, sendQueue);
		}
	}

	// invokes wrap before nio write if SSL enabled
	private int wrapWrite(SocketChannel socketChannel, ByteBuffer unencrypted)
			throws IOException {
		assert (this.isHandshakeComplete(socketChannel));
		if (this.isSSL())
			return ((SSLDataProcessingWorker) this.worker).wrap(socketChannel,
					unencrypted);
		else
			return socketChannel.write(unencrypted);
	}

	private boolean isSSL() {
		return this.worker instanceof SSLDataProcessingWorker ? true : false;
	}
	
	/**
	 * @return SSL mode used by this NIO transport.
	 */
	public SSL_MODES getSSLMode() {
		return this.worker instanceof SSLDataProcessingWorker ? ((SSLDataProcessingWorker) this.worker).sslMode
				: SSL_MODES.CLEAR;
	}

	// invoked only by selector thread
	private boolean isHandshakeComplete(SocketChannel socketChannel) {
		boolean isComplete = isSSL() ? ((SSLDataProcessingWorker) this.worker)
				.isHandshakeComplete(socketChannel) : true;
		if (!isComplete) {
			/*
			 * Deregister write interest, but keep read interest coz we need it
			 * for the handshake itself to complete.
			 */
			SelectionKey key = socketChannel.keyFor(this.selector);
			if (key != null && key.isValid())
				key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
		}
		return isComplete;
	}

	// for application threads to queue sends for selector thread
	private int enqueueSend(InetSocketAddress isa, ByteBuffer data)
			throws IOException {

		int queuedBytes = 0;
		// lock because selector thread may remove sendQueue from sendQueues
		synchronized (this.sendQueues) {
			this.sendQueues.putIfAbsent(isa,
					new LinkedBlockingQueue<ByteBuffer>());
			LinkedBlockingQueue<ByteBuffer> sendQueue = this.sendQueues
					.get(isa);
			if (sendQueue.isEmpty() && (trySneakyWrite(isa, data))
					&& data.remaining() == 0)
				return data.capacity();

			if (sendQueue.size() < getMaxQueuedSends()) {
				sendQueue.add(data);
				queuedBytes = data.capacity();

			} 
			else {
				log.log(Level.WARNING,
						"{0} message queue for {1} out of room, dropping message",
						new Object[] { this, isa });
				queuedBytes = this.isConnected(isa) ? 0 : -1; 
				// could also drop queue here
			}
		}

		if (queuedBytes > 0 && data.remaining() > 0)
			// wake up selecting thread so it can push out the write
			this.wakeupSelector(isa);

		return queuedBytes;
	}
	
	private boolean canEnqueueSend(InetSocketAddress isa) {
		LinkedBlockingQueue<ByteBuffer> sendQueue = null;
		return ((sendQueue = this.sendQueues.get(isa))==null) || sendQueue.size()<MAX_QUEUED_SENDS;
	}
	
	private void wakeupSelector(InetSocketAddress isa) {
		SocketChannel sc = this.getSockAddrToSockChannel(isa);
		SelectionKey key = null;
		/* No point setting op write unless connected and handshaken. If not yet
		 * connected, the selector thread will set op to write when it finishes
		 * connecting.
		 */
		//assert(sc!=null);
		if (sc!=null && sc.isConnected() && this.isHandshakeComplete(sc))
			try {
				// set op to write if not already set
				if ((key = sc.keyFor(this.selector)) != null && key.isValid()
						&& (key.interestOps() & SelectionKey.OP_WRITE) == 0)
					key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			} catch (CancelledKeyException cke) {
				// could have been cancelled upon a write attempt
				cleanupRetry(key, sc, isa);
			}
		this.selector.wakeup();
		// if pending writes and socket closed, retry if possible
		if (sc!=null && !sc.isOpen())
			this.cleanupRetry(null, sc, isa);
	}

	/*
	 * Will try to write directly to the socket if pendingWrites is empty. If
	 * all of it is not written, the rest will get queued to the head (as the
	 * first and only element) of the pendingWrites list.
	 */
	private static final boolean SNEAK_DIRECT_WRITE = true; // default true

	private boolean trySneakyWrite(InetSocketAddress isa, ByteBuffer data) {
		if (!SNEAK_DIRECT_WRITE)
			return false;
		SocketChannel channel = this.getSockAddrToSockChannel(isa);
		if (channel != null && channel.isConnected()
				&& this.isHandshakeComplete(channel)) {
			try {
				this.wrapWrite(channel, data);
				return true;
			} catch (IOException e) {
				if (!this.isDisconnected(isa)) {
					this.updateFailed(isa);
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	/*
	 * Registers a write interest on all sockets that have at least one pending
	 * write. We only call this infrequently to push out writes to destinations
	 * to which connection attempts have failed. If finishConnection fails,
	 * there is no other mechanism to retry establishing a connection to such
	 * destinations.
	 */
	private long lastRegisterWriteInterestsInvoked = 0;
	private static long INTER_REGISTER_WRITE_INTERESTS_SPACING = 8000;

	private void registerWriteInterests() {
		if (System.currentTimeMillis() - this.lastRegisterWriteInterestsInvoked < (Math
				.random() * INTER_REGISTER_WRITE_INTERESTS_SPACING))
			return;
		this.lastRegisterWriteInterestsInvoked = System.currentTimeMillis();
		if (this.sendQueues.isEmpty())
			return;
		synchronized (this.sendQueues) {
			for (InetSocketAddress isa : this.sendQueues.keySet()) {
				LinkedBlockingQueue<ByteBuffer> queue = this.sendQueues
						.get(isa);
				if (queue != null && !queue.isEmpty()) {
					// Nested locking: pendingWrites -> SockAddrToSockChannel
					SocketChannel sc = getSockAddrToSockChannel(isa); // synchronized
					
					/*
					 * We can be here only if an entry was made in queuePending,
					 * which can happen only after initiateConnection, which
					 * maps the isa to a socket channel. Hence, the assert.
					 */
					//assert (sc != null) : isa;
					if(sc==null) return;

					// connected and handshake complete => set op_write
					SelectionKey key = null;
					if (((sc.isConnected() && this.isHandshakeComplete(sc))
							&& (key = sc.keyFor(this.selector)) != null && (key
							.interestOps() & SelectionKey.OP_WRITE) == 0))
						try {
							key.interestOps(key.interestOps()
									| SelectionKey.OP_WRITE);
						} catch (CancelledKeyException cke) {
							// could have been cancelled upon a write attempt
							cleanupRetry(key, sc, isa);
						}
					// if socket closed, retry (if allowed)
					if (!sc.isOpen()) {
						this.cleanupRetry(null, sc, isa);
					}
				}
			}
		}
	}

	/*
	 * Cleans up and suppresses IOException as there is little useful stuff the
	 * selector thread can do at that point.
	 * 
	 * This method is the only one touching selection keys that is invoked by an
	 * entity other than the main selector thread. We could queue these cancel
	 * operations to be processed by the selector thread to preserve the
	 * invariant that all key operations are done only by the selector thread,
	 * but it is not necessary for correctness.
	 */
	private static void cleanup(SelectionKey key, SelectableChannel sc) {
		if (key != null)
			key.cancel();
		try {
			sc.close();
		} catch (IOException ioe) {
			log.warning("IOException encountered while closing socket channel "
					+ sc);
		}
	}

	protected static void cleanup(SelectionKey key) {
		cleanup(key, key.channel());
	}

	/*
	 * Tries to cleanup and re-initiate connection. May fail to re-initiate
	 * connection if the other end has failed. Suppresses exceptions as there is
	 * little the selector thread can do about a failed remote end.
	 */
	private void cleanupRetry(SelectionKey key, SocketChannel sc,
			InetSocketAddress isa) {
		cleanup(key, sc);
		try {
			if (this.canReconnect(isa)) {
				testAndIntiateConnection(isa);
			} // FIXME: should give up eventually

		} catch (IOException ioe) {
			log.warning("IOException encountered while re-initiating connection to "
					+ isa);
		}
	}

	private boolean canReconnect(InetSocketAddress isa) {
		Long last = this.connAttempts.get(isa);
		if (last == null) {
			last = 0L;
		}
		long now = System.currentTimeMillis();
		if (now - last > getMinInterConnectTime()) {
			return true;
		}
		return false;

	}

	private boolean checkAndReconnect(InetSocketAddress isa) {
		boolean canReconnect = false;
		if (this.canReconnect(isa)) {
			this.connAttempts.put(isa, System.currentTimeMillis());
			canReconnect = true;
		}
		return canReconnect;
	}

	/* **************************************************************
	 * End of methods synchronizing on pendingWrites.
	 * ****************************************************************
	 */

	/* **************************************************************
	 * Start of methods synchronizing on SockAddrToSockChannel.
	 * ****************************************************************
	 */

	private SocketChannel getSockAddrToSockChannel(InetSocketAddress isa) {
		synchronized (this.sockAddrToSockChannel) {
			return this.sockAddrToSockChannel.get(isa);
		}
	}

	private void putSockAddrToSockChannel(InetSocketAddress isa,
			SocketChannel socketChannel) {
		synchronized (this.sockAddrToSockChannel) {
			log.log(Level.FINER, "{0} inserting ({1}, {2})", new Object[] {
					this, isa, socketChannel });
			SocketChannel prevChannel = this.sockAddrToSockChannel.put(isa,
					socketChannel);
			if (prevChannel != null) {
				cleanup(prevChannel.keyFor(this.selector), prevChannel);
			}
		}
	}

	/*
	 * Reverse lookup. Should implement a bidirectional hashmap for more
	 * efficiency.
	 */
	private InetSocketAddress getSockAddrFromSockChannel(SocketChannel sc) {
		synchronized (this.sockAddrToSockChannel) {
			InetSocketAddress retval = null;
			if (this.sockAddrToSockChannel.containsValue(sc)) {
				for (InetSocketAddress isa : this.sockAddrToSockChannel
						.keySet()) {
					if (this.sockAddrToSockChannel.get(isa).equals(sc)
							|| this.sockAddrToSockChannel.get(isa) == sc) {
						retval = isa;
					}
				}
			}
			return retval;
		}
	}

	/*
	 * This method will replace an existing connection if any to the destination
	 * with the newly accepted connection and set ops to include write/connect
	 * in addition to reads so as to reuse the accepted connection for writes.
	 */
	private void reuseAcceptedConnectionForWrites(SocketChannel socketChannel) {
		synchronized (this.sockAddrToSockChannel) {
			try {
				this.putSockAddrToSockChannel(
						(InetSocketAddress) socketChannel.getRemoteAddress(),
						socketChannel); // replace existing with newly accepted
				socketChannel.register(this.selector, SelectionKey.OP_READ
				// wait till handshake complete for SSL writes
						| (isSSL() ? 0 : SelectionKey.OP_WRITE));
			} catch (ClosedChannelException e) {
				log.warning("Node"
						+ myID
						+ " failed to set interest ops immediately after accept()");
				// do nothing
			} catch (IOException e) {
				log.warning("Node" + myID + " failed to get remote address");
				// do nothing
			}
		}
	}

	// is connected or is connection pending
	private boolean isConnected(InetSocketAddress isa) {
		SocketChannel sock = null;
		// synchronized (this.sockAddrToSockChannel)
		{
			sock = this.sockAddrToSockChannel.get(isa);
		}
		if (sock != null && (sock.isConnected() || sock.isConnectionPending()))
			return true;
		log.log(Level.FINEST, "{0} socket channel [{1}] not connected",
				new Object[] { this, sock });
		return false;
	}

	/*
	 * Initiate a connection if the existing socket channel is not connected.
	 * Synchronization ensures that the test and connect happen atomically. If
	 * not, we can have additional unused sockets accumulated that can cause
	 * memory leaks over time.
	 */
	private void testAndIntiateConnection(InetSocketAddress isa)
			throws IOException {
		if (!canReconnect(isa) || this.isConnected(isa)) {
			return; // quick check before synchronization
		}
		synchronized (this.sockAddrToSockChannel) {
			if (!this.isConnected(isa) && checkAndReconnect(isa)) {
				log.log(Level.FINE,
						"Node {0} attempting to reconnect to {1}, probably "
								+ "because the remote end died or closed the connection",
						new Object[] { this.myID, isa });
				this.initiateConnection(isa);
			}
		}
	}

	/* **************************************************************
	 * End of methods synchronizing on SockAddrToSockChannel.
	 * ****************************************************************
	 */

	/* **************************************************************
	 * Start of methods synchronizing on pendingConnects.
	 * ****************************************************************
	 */
	private void addPendingConnect(SocketChannel socketChannel) {
		synchronized (this.pendingConnects) {
			this.pendingConnects.add(new ChangeRequest(socketChannel,
					ChangeRequest.REGISTER, SelectionKey.OP_CONNECT));
		}
	}

	/*
	 * Process any pending connect requests to ensure that when the socket is
	 * connectable, finishConnect is called.
	 */
	private void processPendingConnects() {
		if (this.pendingConnects.isEmpty())
			return;
		synchronized (this.pendingConnects) {
			Iterator<ChangeRequest> changes = this.pendingConnects.iterator();
			while (changes.hasNext()) {
				ChangeRequest change = (ChangeRequest) changes.next();
				log.log(Level.FINEST, "{0} processing connect event: {1}",
						new Object[] { this, change });
				SelectionKey key = change.socket.keyFor(this.selector);
				switch (change.type) {
				case ChangeRequest.CHANGEOPS:
					key.interestOps(change.ops);
					break;
				case ChangeRequest.REGISTER:
					try {
						change.socket.register(this.selector, change.ops);
					} catch (ClosedChannelException cce) {
						log.severe("Socket channel likely closed before connect finished");
						cleanup(key, (AbstractSelectableChannel) key.channel());
						cce.printStackTrace();
					}
					break;
				}
			}
			this.pendingConnects.clear();
		}
	}

	/* **************************************************************
	 * End of methods synchronizing on pendingConnects.
	 * ****************************************************************
	 */
	private Selector initSelector(InetSocketAddress mySockAddr)
			throws IOException {
		// Create a new selector
		Selector socketSelector = Selector.open();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);
		serverChannel.socket().setReuseAddress(true);

		InetSocketAddress isa;

		// use sockAddr if null ID
		if (this.myID == null)
			isa = mySockAddr;
		// else if ID is socket address, just use it
		else if (this.myID instanceof InetSocketAddress)
			isa = (InetSocketAddress) this.myID;
		// else get bind address from nodeConfig
		else
			isa = new InetSocketAddress(
					this.nodeConfig.getBindAddress(this.myID),
					this.nodeConfig.getNodePort(this.myID));
		
		try {
			serverChannel.socket().bind(isa);
		} catch (BindException be) {
			log.info(this + " failed to bind to " + isa + "; trying wildcard address instead");
			// try wildcard IP
			serverChannel.socket().bind(new InetSocketAddress(isa.getPort()));
		}
		log.log(Level.FINE, "{0} listening on channel {1}", new Object[]{this, serverChannel});
		
		if (isSSL())
			// only for logging purposes
			((SSLDataProcessingWorker) this.worker).setMyID(myID != null ? myID
					.toString() : isa.toString());

		// Register the server socket channel, indicating an interest in
		// accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}

	protected int getListeningPort() {
		try {
			return ((InetSocketAddress) (this.serverChannel.getLocalAddress()))
					.getPort();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	protected InetAddress getListeningAddress() {
		try {
			return ((InetSocketAddress) (this.serverChannel.getLocalAddress()))
					.getAddress();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * This method will definitely initiate a new connection and replace
	 * existing entries in SockAddrToSockChannel. It is the responsibility of
	 * the caller to check if a connection to isa already exists.
	 */
	private SocketChannel initiateConnection(InetSocketAddress isa)
			throws IOException {
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
		socketChannel.socket().setSendBufferSize(HINT_SOCK_BUFFER_SIZE);
		socketChannel.socket().setReceiveBufferSize(HINT_SOCK_BUFFER_SIZE);

		// Kick off connection establishment
		log.log(Level.FINE, "{0} connecting to socket address {1}",
				new Object[] { this, isa });

		socketChannel.socket().setSoLinger(false, -1);
		socketChannel.socket().setTcpNoDelay(true);
		socketChannel.connect(isa);
		NIOInstrumenter.incrInitiated();
		putSockAddrToSockChannel(isa, socketChannel); // synchronized
		// should verify that there are no partial byte buffers
		removePartialBuffers(isa);

		// Queue a channel registration since the caller is not the
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		addPendingConnect(socketChannel); // synchronized

		return socketChannel;
	}

	/*
	 * Will remove partial buffers at connection initiation time for the
	 * following reason. The reader at a receiver attaches a byte buffer with a
	 * socket channel, not an InetSocketAddress, and that is the way it must be
	 * as the sender's socket address may change arbitrary when it reconnects
	 * after a connection failure. In such failure cases, the buffer at the head
	 * of the queue may have been sent partially on the previous (failed)
	 * connection. If we don't remove the rest of this unsent head buffer, we
	 * will continue to parse illegal payload lengths resulting in even more IO
	 * exceptions.
	 * 
	 * Implication: Such removals means that we do not strictly maintain
	 * reliable, byte stream semantics upon connection failures. What we do
	 * maintain is an unreliable, ordered buffer stream, i.e., a stream of byte
	 * buffers that are received in the same order as the order in which they
	 * were sent with the caveat that some buffers may be missing from the
	 * received stream if connection failures happen. Each received buffer is
	 * nevertheless guaranteed to be reliable. The missing buffers correspond
	 * exactly to the buffers (partially or wholly) written to the underlying
	 * TCP socket but not yet sent to the other end.
	 * 
	 */
	private void removePartialBuffers(InetSocketAddress isa) {
		LinkedBlockingQueue<ByteBuffer> sendQueue = this.sendQueues.get(isa);
		if (sendQueue == null || sendQueue.isEmpty())
			return;
		synchronized (sendQueue) {
			ByteBuffer bbuf = ByteBuffer.allocate(HEADER_SIZE);
			Util.put(bbuf, sendQueue.peek());
			sendQueue.peek().rewind();
			bbuf.flip();
			int length = -1;
			try {
				length = getPayloadLength(bbuf);
			} catch(IOException ioe) {
				assert (outOfRange(length) || (length != sendQueue.peek()
						.capacity() - HEADER_SIZE));
				if (sendQueue.remove() != null)
					log.severe(this
							+ " initiated connection and removed partial unsent packet in send queue to "
							+ isa);
			}
		}
	}

	protected static boolean outOfRange(int length) {
		return length < 0 || length > MAX_PAYLOAD_SIZE;
	}

	/*
	 * Invoked only by the selector thread. Sets selection ops to write as the
	 * connection is presumably being set up to write something.
	 */
	private boolean finishConnection(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		// For Java6, this line and the log.warning below can both be commented
		// without affecting functionality
		boolean connected = false;

		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			connected = socketChannel.finishConnect()
					&&
					// will register only if finishConnect() is true
					this.registerSSL(
							key.interestOps(
							// writes allowed only after handshake complete
							(this.isHandshakeComplete(socketChannel) ? SelectionKey.OP_WRITE
									: 0)
									// reads allowed and in fact needed right
									// away
									| SelectionKey.OP_READ), true);
			// duplex connection => we may have to read replies
			key.attach(new AlternatingByteBuffer());
			if(connected) this.updateAlive((SocketChannel)key.channel());
		} catch (IOException e) {
			InetSocketAddress isa = new InetSocketAddress(socketChannel.socket()
					.getInetAddress(), socketChannel.socket()
					.getPort());
			// cancel the channel's registration with selector
			log.log(Level.INFO, "Node {0} failed to (re-)connect to {1}:{2}",
					new Object[] {
							this.myID,
							isa, e.getMessage() });
			cleanup(key, socketChannel);
			// clearPending will also drop outstanding data here
			if (!this.isNodeID(isa) || Util.oneIn(NUM_RETRIES))
				this.clearPending(socketChannel);
			connected = false;
		}
		if (connected)
			log.log(Level.FINEST, "{0} finished connecting {1}", new Object[] {
					this, socketChannel });
		return connected;
	}
	private static final int NUM_RETRIES = 100;
	
	private boolean isNodeID(InetSocketAddress isa) {
		if (this.nodeConfig == null)
			return false;
		for (NodeIDType node : this.nodeConfig.getNodeIDs())
			if (isa.getAddress().equals(this.nodeConfig.getNodeAddress(node))
					&& isa.getPort() == this.nodeConfig.getNodePort(node))
				return true;
		return false;
	}

	private boolean registerSSL(SelectionKey key, boolean client)
			throws IOException {
		return (isSSL()) ? ((SSLDataProcessingWorker) this.worker).register(
				key, client) : true;
	}

	public String toString() {
		return this.getClass().getSimpleName() + (this.myID!=null ? this.myID : "");
	}

	/*
	 * This method notifies the selector thread that a handshake is complete so
	 * that in the next iteration, it can try to wrapWrite queued (unencrypted)
	 * outgoing data.
	 */
	@Override
	public void handshakeComplete(SelectionKey key) {
		try {
			this.wakeupSelector((InetSocketAddress) ((SocketChannel) key
					.channel()).getRemoteAddress());
		} catch (IOException e) {
			log.severe(this
					+ " encountered IOException upon handshake completion for "
					+ key.channel());
			cleanup(key);
		}
	}

	// A utility container class.
	private class ChangeRequest {

		static final int REGISTER = 1;
		static final int CHANGEOPS = 2;

		final SocketChannel socket;
		final int type;
		final int ops;

		ChangeRequest(SocketChannel socket, int type, int ops) {
			this.socket = socket;
			this.type = type;
			this.ops = ops;
		}

		public String toString() {
			return "" + socket + ":" + type + ":" + ops;
		}
	}

	/* ********* Testing methods below ********************* */

	/*
	 * Used only for testing to print pending messages if any at the end of
	 * tests.
	 */
	protected int getPendingSize() {
		synchronized (this.sendQueues) {
			int numPending = 0;
			for (LinkedBlockingQueue<ByteBuffer> arr : this.sendQueues.values()) {
				numPending += arr.size();
			}
			return numPending;
		}
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.FINEST);
		Logger log = Logger.getLogger(NIOTransport.class.getName());
		log.addHandler(handler);
		log.setLevel(Level.INFO);

		int port = 2000;
		int nNodes = 100;
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(port);
		snc.localSetup(nNodes + 1);
		DataProcessingWorkerDefault worker = new DataProcessingWorkerDefault();
		NIOTransport<?>[] niots = new NIOTransport[nNodes];

		try {
			int smallNNodes = 2;
			for (int i = 0; i < smallNNodes; i++) {
				niots[i] = new NIOTransport<Integer>(i, snc, worker);
				new Thread(niots[i]).start();
			}

			/*
			 * Test a few simple hellos. The sleep is there to test that the
			 * successive writes do not "accidentally" benefit from concurrency,
			 * i.e., to check that write ops flags will be set correctly.
			 */
			((NIOTransport<Integer>) niots[1]).send(0,
					"Hello from 1 to 0".getBytes());
			((NIOTransport<Integer>) niots[0]).send(1,
					"Hello back from 0 to 1".getBytes());
			((NIOTransport<Integer>) niots[0]).send(1,
					"Second hello back from 0 to 1".getBytes());
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			((NIOTransport<Integer>) niots[0]).send(1,
					"Third hello back from 0 to 1".getBytes());
			((NIOTransport<Integer>) niots[1]).send(0,
					"Thank you for all the hellos back from 1 to 0".getBytes());

			Thread.sleep(2000);
			System.out
					.println("\n\n\nBeginning test of random, sequential communication pattern");
			Thread.sleep(1000);

			// Create the remaining nodes up to nNodes
			for (int i = smallNNodes; i < nNodes; i++) {
				niots[i] = new NIOTransport<Integer>(i, snc, worker);
				new Thread(niots[i]).start();
			}

			// Test a random, sequential communication pattern
			for (int i = 0; i < nNodes; i++) {
				int k = (int) (Math.random() * nNodes);
				if (k >= nNodes) {
					k = nNodes - 1;
				}
				int j = (int) (Math.random() * nNodes);
				System.out.println("Sending message " + i + " from " + k
						+ " to " + j);
				((NIOTransport<Integer>) niots[k]).send(j, ("Hello from " + k
						+ " to " + j).getBytes());
			}

			Thread.sleep(1000);
			System.out
					.println("\n\n\nBeginning test of random, concurrent 1-to-1 communication pattern");
			Thread.sleep(1000);

			// Test a random, concurrent communication pattern
			ScheduledExecutorService execpool = Executors
					.newScheduledThreadPool(5);

			class TX extends TimerTask {

				NIOTransport<Integer> sndr = null;
				private int rcvr = -1;

				TX(int i, int id, NIOTransport<?>[] n) {
					sndr = ((NIOTransport<Integer>) n[i]);
					rcvr = id;
				}

				TX(NIOTransport<Integer> niot, int id) {
					sndr = niot;
					rcvr = id;
				}

				public void run() {
					try {
						sndr.send(rcvr,
								("Hello from " + sndr.myID + " to " + rcvr)
										.getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			NIOTransport<Integer> concurrentSender = new NIOTransport<Integer>(
					nNodes, snc, worker);
			new Thread(concurrentSender).start();
			ScheduledFuture<?>[] futuresRandom = new ScheduledFuture[nNodes];
			for (int i = 0; i < nNodes; i++) {
				TX task = new TX(concurrentSender, 0);
				System.out.println("Scheduling random message " + i
						+ " from concurrentSender to " + 0);
				futuresRandom[i] = execpool.schedule(task, 0,
						TimeUnit.MILLISECONDS);
			}
			for (int i = 0; i < nNodes; i++) {
				try {
					futuresRandom[i].get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			Thread.sleep(1000);
			System.out
					.println("\n\n\nBeginning test of random, concurrent, any-to-any communication pattern");
			Thread.sleep(1000);

			int load = nNodes * 50;
			ScheduledFuture<?>[] futures = new ScheduledFuture[load];
			for (int i = 0; i < load; i++) {
				int k = (int) (Math.random() * nNodes);
				if (k >= nNodes) {
					k = nNodes - 1;
				}
				int j = (int) (Math.random() * nNodes);
				long millis = (long) (Math.random() * 1000);
				if (i % 100 == 0) {
					j = nNodes; // Periodically try sending to a
								// non-existent node
				}
				TX task = new TX(k, j, niots);
				System.out.println("Scheduling random message " + i + " from "
						+ k + " to " + j);
				futures[i] = (ScheduledFuture<?>) execpool.schedule(task,
						millis, TimeUnit.MILLISECONDS);
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

			Thread.sleep(4000);
			System.out
					.println("\n\n\nPrinting overall stats. Number of exceptions =  "
							+ numExceptions);
			System.out.println("NIO " + (new NIOInstrumenter()));
			for (NIOTransport<?> niot : niots) {
				niot.stop();
			}
			concurrentSender.stop();
			execpool.shutdown();

			System.out
					.println("\nTesting notes: The number of missing-or-batched messages should be small. The number of\n"
							+ " missing-or-batched messages may be nonzero as two back-to-back messages may get\n"
							+ " counted as one, as in the very first test above. With concurrent send tests or node failures,\n"
							+ " missing-or-batched may be a nontrivial fraction of totalSent. \nTBD: an exact"
							+ " success/failure outputting test. For now, try testing JSONNIOTransport instead.");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
