package edu.umass.cs.gns.nio;

import edu.umass.cs.gns.nsdesign.nodeconfig.InterfaceNodeConfig;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.nioutils.DataProcessingWorkerDefault;
import edu.umass.cs.gns.nio.nioutils.NIOInstrumenter;

import edu.umass.cs.gns.nsdesign.nodeconfig.SampleNodeConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author V. Arun
 */

/*
 * This class is designed for NIO transport. The nice thing about this
 * class is that all steps are truly asynchronous. Even connect calls.
 * There is no blocking on connect, read, or write anywhere and no
 * polling in the code.
 * 
 * The code can be used to send a byte stream to a numbered node
 * (with ID->InetSocketAddress mappings specified using the NodeConfig
 * interface) or to directly send messages to an InetSocketAddress.
 * Received data is processed using the DataProcessingWorker interface.
 * Received data can not be associated with a node ID as the received data
 * just looks like a byte stream to the receiving node. The supplied
 * DataProcessingWorker is expected to know what to do with the
 * received byte streams.
 * 
 * The high-level organization of this code has a selector thread that
 * waits for connect, accept, read, or write events on socket channels
 * that could result from accepting connections (server side) or from
 * initiating connections upon send requests by application threads.
 * The selector thread also writes to socket channels and reads incoming
 * data from socket channels and passes them off to DataProcessingWorker.
 * The selector thread is the only thread to touch socket channel selection
 * keys, e.g., to change ops or select() on them. Other application threads
 * may invoke send() concurrently to send data. This data is queued in
 * pendingWrites, a synchronized structure between application threads and
 * the selector thread. A pending write is associated with an InetSocketAddress.
 * So, the selector thread can always re-establish a connection to continue
 * writing the byte stream in case the existing connection fails. The
 * selector thread reads from pendingWrites and sets selection ops to
 * wait for write-ready events accordingly.
 * 
 * To enable non-blocking connects, the application threads queue connect
 * events in the synchronized structure pendingConnects. The selector
 * thread reads from pendingConnects and sets selection ops to wait
 * for a connect event as needed.
 * 
 * A map SockAddrToSockChannel keeps track of the current socket channel
 * being used to send data to a given InetSocketAddress. Note that this
 * mapping can change if connections fail and are re-established. A failed
 * connection can get re-established on demand by an application thread
 * or by the selector thread when it tries to actually write the data to
 * a socket channel and encounters an exception.
 */
public class NIOTransport<NodeIDType> implements Runnable {
	public static final boolean DEBUG = false;
	public static final boolean LOCAL_LOGGER = true;

	private static final double LOG_SAMPLING_FRACTION = 0.1;

	/*
	 * number of sends that can be queued because the connection
	 * was established but the remote end crashed before the
	 * send was complete.
	 */
	protected static final int MAX_QUEUED_SENDS = 8192;

	protected static final int MIN_INTER_CONNECT_TIME = 5000; // milliseconds before reconnection attempts

	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	private Selector selector;

	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(8192);

	protected InterfaceDataProcessingWorker worker;

	// A list of pending connect calls on which finishConnect needs to be called.
	private LinkedList<ChangeRequest> pendingConnects = null;

	/*
	 * The key is a socket address and the value is
	 * a list of messages to be sent to that socket address.
	 */
	private HashMap<InetSocketAddress, ArrayList<ByteBuffer>> pendingWrites =
			null;

	/*
	 * Maps a socket address to a socket channel. The latter may change in case
	 * a connection breaks and a new one needs to be initiated.
	 */
	private HashMap<InetSocketAddress, SocketChannel> SockAddrToSockChannel =
			null;

	/* Map to optimize connection attempts by the selector thread. */
	private ConcurrentHashMap<InetSocketAddress, Long> connAttempts = null;

	// Maps id to socket address
	private InterfaceNodeConfig<NodeIDType> nodeConfig = null;

	/* An id corresponds to a socket address as specified in NodeConfig */
	protected NodeIDType myID;

	private boolean started = false;

	private boolean stopped = false;

	public static Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(NIOTransport.class.getName())
					: GNS.getLogger();

	public NIOTransport(NodeIDType id, InterfaceNodeConfig<NodeIDType> nc,
			InterfaceDataProcessingWorker worker) throws IOException {
		this.myID = id;
		this.nodeConfig = (nc != null ? nc : new SampleNodeConfig<NodeIDType>()); // null node config means no IDs
		this.selector = this.initSelector();
		this.worker = worker;

		this.pendingConnects = new LinkedList<ChangeRequest>();
		this.pendingWrites =
				new HashMap<InetSocketAddress, ArrayList<ByteBuffer>>();
		this.SockAddrToSockChannel =
				new HashMap<InetSocketAddress, SocketChannel>();
		this.connAttempts = new ConcurrentHashMap<InetSocketAddress, Long>();
	}

	/*
	 * send() methods are called by external application threads. They may
	 * initiate a connection if one is not available. However, the connection
	 * is finished in a non-blocking manner by the selector thread. Data to be
	 * sent is queued in pendingWrites, which is read later by the selector thread.
	 */
	public int send(NodeIDType id, byte[] data) throws IOException {
		if (DEBUG)
			log.fine("Node " + myID + " invoked send (" + id + ", " +
					new String(data) + "), checking connection status..");
		return send(new InetSocketAddress(this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)), data);
	}

	public int send(InetSocketAddress isa, byte[] data) throws IOException {
		testAndIntiateConnection(isa);
		NIOInstrumenter.incrSent();
		int written = this.queuePendingWrite(isa, data);
		// Finally, wake up our selecting thread so it can make the required changes
		this.selector.wakeup();
		return written;
	}

	public void run() {
		this.started = true;
		while (!isStopped()) {
			try {
				// Set ops to WRITE for pending write requests.
				registerWriteInterests(); // synchronized
				// Set ops to CONNECT for pending connect requests.
				processPendingConnects(); // synchronized
				// Wait for an event one of the registered channels.
				this.selector.select();
				// Accept, connect, read, or write as needed.
				processSelectedKeys();
			} catch (Exception e) {
				/*
				 * Can do little else here. Hopefully, the exceptions
				 * inside the individual methods above have already
				 * been contained within them.
				 */
				e.printStackTrace();
			}
		}
		try {
			this.selector.close();
			this.serverChannel.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public boolean isStarted() {
		return this.started;
	}

	public synchronized void stop() {
		this.stopped = true;
		this.selector.wakeup();
	}

	public synchronized boolean isStopped() {
		return this.stopped;
	}

	protected InetAddress getNodeAddress() {
		return this.myID!=null ? this.nodeConfig.getNodeAddress(myID) : getListeningAddress();
	}
	protected int getNodePort() {
		return this.myID!=null ? this.nodeConfig.getNodePort(myID) : this.getListeningPort();
	}
	

	/**
	 * ********** Start of private methods ***************************
	 */
	// Invoked only by the selector thread. Typical nio event handling code.
	private void processSelectedKeys() {
		// Iterate over the set of keys for which events are available
		Iterator<SelectionKey> selectedKeys =
				this.selector.selectedKeys().iterator();

		while (selectedKeys.hasNext()) {
			SelectionKey key = (SelectionKey) selectedKeys.next();
			selectedKeys.remove();

			if (!key.isValid()) {
				continue;
			}
			try {
				// Check what event is available and deal with it
				if (key.isAcceptable()) {
					this.accept(key);
				} else if (key.isConnectable()) {
					this.finishConnection(key);
				} else if (key.isReadable()) {
					this.read(key);
				} else if (key.isWritable()) {
					this.write(key);
				}
			} catch (IOException ioe) {
				log.severe("IOException encountered while processing key: " +
						key);
				ioe.printStackTrace(); // print and move on with other keys
			}
		}
	}

	/*
	 * Invoked only by the selector thread.
	 * accept immediately sets the channel in read mode as a connection
	 * is presumably being established to send some data. We could also
	 * insert a mapping into SockAddrToSockChannel, but this is unlikely
	 * to be useful as responses will be addressed to the destination's
	 * InetSocketAddress, which would not be the same as the remote
	 * address of the socket channel just established.
	 */
	private void accept(SelectionKey key) throws IOException {

		// For an accept to be pending the channel must be a server socket channel.
		ServerSocketChannel serverSocketChannel =
				(ServerSocketChannel) key.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		if (DEBUG)
			log.fine("Node " + this.myID + " accepted connection from " +
					socketChannel.getRemoteAddress());
		NIOInstrumenter.incrAccepted();
		socketChannel.configureBlocking(false);
		socketChannel.socket().setKeepAlive(true);

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ);
	}

	/*
	 * Invoked only by the selector thread.
	 * read() is easy as it just needs to read whatever is available
	 * and send it off to DataProcessingWorker (that has to deal with
	 * the complexity of parsing a byte stream).
	 */
	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		assert (socketChannel != null) : "Null socketChannel registered to key " +
				key;

		// Clear out our read buffer so it's ready for new data
		this.readBuffer.clear();

		// Attempt to read off the channel
		int numRead = -1;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} finally {
			// The remote forcibly or cleanly closed the connection.
			// cancel the selection key and close the channel.
			if (numRead == -1) {
				cleanup(key, socketChannel);
			} else {
				NIOInstrumenter.incrRcvd();
			}
		}

		// Hand the data off to our worker thread
		if (numRead > 0) {
			this.worker.processData(socketChannel, this.readBuffer.array(),
					numRead);
		}
	}

	/*
	 * Invoked only by the selector thread. If a write encounters an exception,
	 * the selector thread may establish a new connection.
	 */
	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		InetSocketAddress isa = getSockAddrFromSockChannel(socketChannel);
		/*
		 * At this point, isa can be null as follows. The selector thread
		 * tries to write to a socket channel only if a write op interest
		 * was previously registered. A write op interest is registered
		 * upon either a finishConnection or upon finding an entry in
		 * pendingWrites. A null isa here means that a write op
		 * interest was registered on some socket channel either in
		 * finishConnection or registerWriteInterests, but the
		 * corresponding isa was subsequently remapped to a different
		 * socket channel before the socket channel's key was canceled by
		 * the selector thread. This can happen if some app calls send
		 * and, by consequence, testAndInitiateConnection multiple times
		 * in quick succession to a failed node and the selector thread
		 * has not had a chance to call cleanup() and cancel the key
		 * corresponding to an earlier socket channel.
		 * 
		 * Such orphaned sockets, i.e., those without an isa, can be
		 * cleaned up as the corresponding isa would be mapped to a
		 * different socket channel that will be registered for writes
		 * by registerWriteInterests if there are any outstanding
		 * writes to that isa.
		 */
		if (isa == null) {
			log.severe("Null socket address for a write-ready socket!");
			this.cleanup(key, socketChannel);
		} else {
			try {
				// If all data gets written successfully, switch back to read mode.
				if (this.writeAllPendingWrites(isa, socketChannel)) {
					key.interestOps(SelectionKey.OP_READ); // synchronized
				}
			} catch (IOException e) {
				this.cleanupRetry(key, socketChannel, isa); // Will close socket channel and retry (but may still fail)
			}
		}
	}

	/* **************************************************************
	 * Start of methods synchronizing on pendingWrites.
	 * ****************************************************************
	 */

	/*
	 * Invoked only by the selector thread.
	 * Writes to socket channel and de-queues writes. If an exception occurs,
	 * it lets the bytes remain in the queue. However, we don't know if or how
	 * many of those bytes got sent to the other side. Basically, socket exceptions
	 * are bad news and will likely result in some missing data.
	 */
	private boolean writeAllPendingWrites(InetSocketAddress isa,
			SocketChannel socketChannel) throws IOException {
		synchronized (this.pendingWrites) {
			ArrayList<ByteBuffer> queue =
					(ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
			// queue can be null if queuePendingWrite has not yet happened after connect
			if (queue == null) {
				return true;
			}
			// Write until there's not more data ...
			while (!queue.isEmpty()) {
				ByteBuffer buf = (ByteBuffer) queue.get(0);
				socketChannel.write(buf);
				// If the socket's buffer fills up, let the rest be in queue
				if (DEBUG)
					log.finest("Node " + this.myID + " wrote: " +
							new String(queue.get(0).array()) + " to " + isa);
				if (buf.remaining() > 0) {
					log.warning("Socket buffer congested because of high load..");
					break;
				}
				queue.remove(0);
			}
			// We wrote away all data, so we're no longer interested in
			// writing on this socket. Switch back to waiting for data.
			if (queue.isEmpty()) {
				return true;
			} else {
				return false;
			}
		}
	}

	/*
	 * Invoked by application threads so that the selector thread can
	 * process them.
	 */
	private int queuePendingWrite(InetSocketAddress isa, byte[] data)
			throws IOException {
		synchronized (this.pendingWrites) {
			int queuedBytes = 0;
			ArrayList<ByteBuffer> queue =
					(ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
			if (queue == null) {
				queue = new ArrayList<ByteBuffer>();
				this.pendingWrites.put(isa, queue);
			}
			if (queue.size() < NIOTransport.MAX_QUEUED_SENDS) {
				queue.add(ByteBuffer.wrap(data));
				queuedBytes = +data.length;
				if (DEBUG)
					log.finest("Node " + this.myID + " queued: " +
							new String(data));
			} else {
				if (sampleLog())
					log.warning("Node " + this.myID + "'s queue for " + isa +
							" too full, dropping message");
				if (!this.isConnected(isa))
					queuedBytes = -1; // could also drop queue here
			}
			return queuedBytes;
		}
	}

	public static boolean sampleLog() {
		return Math.random() > 1 - LOG_SAMPLING_FRACTION;
	}

	/*
	 * Invoked only by the selector thread.
	 * Register a write interest on all sockets that have at least one
	 * pending write. Note that it is not enough to register a write
	 * interest just once unless you can ensure that the interest will
	 * be converted to a successful write; if not, writes can get missed.
	 */
	private void registerWriteInterests() {
		synchronized (this.pendingWrites) {
			for (InetSocketAddress isa : this.pendingWrites.keySet()) {
				ArrayList<ByteBuffer> queue =
						(ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
				if (queue != null && !queue.isEmpty()) {
					// Nested locking: pendingWrites -> SockAddrToSockChannel
					SocketChannel sc = getSockAddrToSockChannel(isa); // synchronized
					/*
					 * We can be here only if an entry was made in queuePending,
					 * which can happen only after initiateConnection, which maps
					 * the isa to a socket channel. Hence, the assert.
					 */
					assert (sc != null);
					SelectionKey key =
							(sc != null &&
									(sc.isConnected() || canReconnect(isa)) ? sc.keyFor(this.selector)
									: null);
					try {
						if (key != null) {
							key.interestOps(SelectionKey.OP_WRITE |
									SelectionKey.OP_CONNECT);
						}
					} catch (CancelledKeyException cke) { // Could have been cancelled upon a write attempt
						cleanupRetry(key, sc, isa);
					}
				}
			}
		}
	}

	/*
	 * Cleans up and suppresses IOException as there is little
	 * useful stuff the selector thread can do at that point.
	 */

	private void cleanup(SelectionKey key, SocketChannel sc) {
		key.cancel();
		try {
			sc.close();
		} catch (IOException ioe) {
			log.warning("IOException encountered while closing socket channel " +
					sc);
		}
	}

	/*
	 * Tries to cleanup and re-initiate connection. May fail to re-initiate
	 * connection if the other end has failed. Suppresses exceptions as there
	 * is little the selector thread can do about a failed remote end.
	 */

	private void cleanupRetry(SelectionKey key, SocketChannel sc,
			InetSocketAddress isa) {
		cleanup(key, sc);
		try {
			if (this.checkAndReconnect(isa)) {
				testAndIntiateConnection(isa);
			}
		} catch (IOException ioe) {
			log.warning("IOException encountered while re-initiating connection to " +
					isa);
		}
	}

	private boolean canReconnect(InetSocketAddress isa) {
		Long last = this.connAttempts.get(isa);
		if (last == null) {
			last = 0L;
		}
		long now = System.currentTimeMillis();
		if (now - last > NIOTransport.MIN_INTER_CONNECT_TIME)
			return true;
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

	/*
	 * Reverse lookup. Should implement a bidirectional hashmap for
	 * more efficiency.
	 */
	private InetSocketAddress getSockAddrFromSockChannel(SocketChannel sc) {
		synchronized (this.SockAddrToSockChannel) {
			InetSocketAddress retval = null;
			if (this.SockAddrToSockChannel.containsValue(sc)) {
				for (InetSocketAddress isa : this.SockAddrToSockChannel.keySet()) {
					if (this.SockAddrToSockChannel.get(isa).equals(sc) ||
							this.SockAddrToSockChannel.get(isa) == sc) {
						retval = isa;
					}
				}
			}
			return retval;
		}
	}

	private SocketChannel getSockAddrToSockChannel(InetSocketAddress isa) {
		synchronized (this.SockAddrToSockChannel) {
			return this.SockAddrToSockChannel.get(isa);
		}
	}

	private void putSockAddrToSockChannel(InetSocketAddress isa,
			SocketChannel socketChannel) {
		synchronized (this.SockAddrToSockChannel) {
			if (DEBUG)
				log.finer("Node " + myID + " inserting (" + isa + ", " +
						socketChannel + ")");
			this.SockAddrToSockChannel.put(isa, socketChannel);
		}
	}

	private boolean isConnected(InetSocketAddress isa) {
		synchronized (this.SockAddrToSockChannel) {
			SocketChannel sock =
					(SocketChannel) this.SockAddrToSockChannel.get(isa);
			if (sock != null &&
					(sock.isConnected() || sock.isConnectionPending())) {
				return true;
			}
			if (DEBUG)
				log.finest("Node " + myID + " socket channel [" + sock +
						"] not connected");
			return false;
		}
	}

	/*
	 * Initiate a connection if the existing socket channel is not connected.
	 * Synchronization ensures that the test and connect happen atomically.
	 * If not, we can have additional unused sockets accumulated that can
	 * cause memory leaks over time.
	 */
	private SocketChannel testAndIntiateConnection(InetSocketAddress isa)
			throws IOException {
		synchronized (this.SockAddrToSockChannel) {
			SocketChannel sock = null;
			if (!this.isConnected(isa)) {
				SocketChannel oldSock = this.getSockAddrToSockChannel(isa); // synchronized
				if (oldSock != null && this.canReconnect(isa)) {
					log.log(Level.WARNING,
							"Node {0} finds channel to {1} dead, probably "
									+ "because the remote end died or closed the connection",
							new Object[] { this.myID, isa });
				}
				if (checkAndReconnect(isa))
					sock = this.initiateConnection(isa);
			}
			return sock;
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
	 * Process any pending connect requests to ensure that when the socket
	 * is connectable, finishConnect is called.
	 */

	private void processPendingConnects() {
		synchronized (this.pendingConnects) {
			Iterator<ChangeRequest> changes = this.pendingConnects.iterator();
			while (changes.hasNext()) {
				ChangeRequest change = (ChangeRequest) changes.next();
				if (DEBUG)
					log.fine("Node " + myID + " processing connect event : " +
							change);
				switch (change.type) {
				case ChangeRequest.CHANGEOPS:
					SelectionKey key = change.socket.keyFor(this.selector);
					key.interestOps(change.ops);
					break;
				case ChangeRequest.REGISTER:
					try {
						change.socket.register(this.selector, change.ops);
					} catch (ClosedChannelException cce) {
						log.severe("Socket channel likely closed before connect finished");
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
	private Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

                InetSocketAddress isa;
                
                //FIXME: make this a little less hackish
                if (this.myID == null) {
                  isa = new InetSocketAddress((InetAddress)null, 0);
//                } else if (myID instanceof NodeId) {
//		// Bind the server socket to the specified address and port
//                  isa = new InetSocketAddress(this.nodeConfig.getNodeAddress(this.myID),
//                                              this.nodeConfig.getNodePort(this.myID));
                } else if (myID instanceof String) {
		// Bind the server socket to the specified address and port
                  isa = new InetSocketAddress(this.nodeConfig.getNodeAddress(this.myID),
                                              this.nodeConfig.getNodePort(this.myID));
                } else if (myID instanceof InetSocketAddress){
		  isa = (InetSocketAddress) myID;
                } else {
                  throw new IOException("Unknown type for Node id " + myID.getClass());
                }
                serverChannel.socket().bind(isa); 
		log.info("LNS listening on " + isa);							;  

		// Register the server socket channel, indicating an interest in
		// accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}
	protected int getListeningPort() {
		try {
			return ((InetSocketAddress)(this.serverChannel.getLocalAddress())).getPort();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	protected InetAddress getListeningAddress() {
		try {
			return ((InetSocketAddress)(this.serverChannel.getLocalAddress())).getAddress();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	// FIXME: Unused. Either use or remove.
	protected boolean isConnected(NodeIDType id) {
		return isConnected(new InetSocketAddress(
				this.nodeConfig.getNodeAddress(id),
				this.nodeConfig.getNodePort(id)));
	}

	/*
	 * This method will definitely initiate a new connection and replace existing
	 * entries in SockAddrToSockChannel. It is the responsibility of the caller
	 * to check if a connection to isa already exists.
	 */
	private SocketChannel initiateConnection(InetSocketAddress isa)
			throws IOException {
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);

		// Kick off connection establishment
		if (DEBUG)
			log.finer("Node " + myID + " connecting to socket address " + isa);
		socketChannel.connect(isa);
		NIOInstrumenter.incrInitiated();
		putSockAddrToSockChannel(isa, socketChannel); // synchronized

		// Queue a channel registration since the caller is not the
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		addPendingConnect(socketChannel); // synchronized

		return socketChannel;
	}

	/*
	 * Invoked only by the selector thread.
	 * Sets selection ops to write as the connection is presumably being
	 * set up to write something.
	 */
	private boolean finishConnection(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		// For Java6, this line and the log.warning below can both be commented without affecting functionality
		SocketAddress isa = socketChannel.getRemoteAddress();
		boolean connected = false;

		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			connected = socketChannel.finishConnect();
		} catch (IOException e) {
			// Cancel the channel's registration with our selector
			log.warning("Node " + this.myID + " failed to connect to " + isa);
			// FIXME: Should probably drop outstanding data here
			this.cleanup(key, socketChannel);
		}

		/*
		 * Register an interest in writing on this channel. No
		 * point registering a write interest until the socket
		 * is connected as the write will block anyway.
		 */
		if (connected) {
			if (DEBUG)
				log.fine("Node " + myID + " finished connecting " +
						socketChannel);
		}
		if (connected) {
			key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT);
		}
		return connected;
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

	/**
	 * ************************* Testing methods below *********************************
	 */
	/*
	 * Used only for testing to print pending messages if any
	 * at the end of tests.
	 */
	public int getPendingSize() {
		synchronized (this.pendingWrites) {
			int numPending = 0;
			for (ArrayList<ByteBuffer> arr : this.pendingWrites.values()) {
				numPending += arr.size();
			}
			return numPending;
		}
	}

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

			/**
			 * **********************************************************************
			 */
			/*
			 * Test a few simple hellos. The sleep is there to test
			 * that the successive writes do not "accidentally" benefit
			 * from concurrency, i.e., to check that write ops flags will
			 * be set correctly.
			 */
			((NIOTransport<Integer>)niots[1]).send(0, "Hello from 1 to 0".getBytes());
			((NIOTransport<Integer>)niots[0]).send(1, "Hello back from 0 to 1".getBytes());
			((NIOTransport<Integer>)niots[0]).send(1, "Second hello back from 0 to 1".getBytes());
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			((NIOTransport<Integer>)niots[0]).send(1, "Third hello back from 0 to 1".getBytes());
			((NIOTransport<Integer>)niots[1]).send(0,
					"Thank you for all the hellos back from 1 to 0".getBytes());
			/**
			 * **********************************************************************
			 */

			Thread.sleep(2000);
			System.out.println("\n\n\nBeginning test of random, sequential communication pattern");
			Thread.sleep(1000);

			/**
			 * **********************************************************************
			 */
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
				System.out.println("Sending message " + i + " from " + k +
						" to " + j);
				((NIOTransport<Integer>)niots[k]).send(j, ("Hello from " + k + " to " + j).getBytes());
			}
			/**
			 * **********************************************************************
			 */
			Thread.sleep(1000);
			System.out.println("\n\n\nBeginning test of random, concurrent 1-to-1 communication pattern");
			Thread.sleep(1000);
			/**
			 * **********************************************************************
			 */
			// Test a random, concurrent communication pattern
			ScheduledExecutorService execpool =
					Executors.newScheduledThreadPool(5);

			class TX extends TimerTask {

				NIOTransport<Integer> sndr = null;
				private int rcvr = -1;

				TX(int i, int id, NIOTransport<?>[] n) {
					sndr = ((NIOTransport<Integer>)n[i]);
					rcvr = id;
				}

				TX(NIOTransport<Integer> niot, int id) {
					sndr = niot;
					rcvr = id;
				}

				public void run() {
					try {
						sndr.send(
								rcvr,
								("Hello from " + sndr.myID + " to " + rcvr).getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			NIOTransport<Integer> concurrentSender =
					new NIOTransport<Integer>(nNodes, snc, worker);
			new Thread(concurrentSender).start();
			ScheduledFuture<?>[] futuresRandom = new ScheduledFuture[nNodes];
			for (int i = 0; i < nNodes; i++) {
				TX task = new TX(concurrentSender, 0);
				System.out.println("Scheduling random message " + i +
						" from concurrentSender to " + 0);
				futuresRandom[i] =
						execpool.schedule(task, 0, TimeUnit.MILLISECONDS);
			}
			for (int i = 0; i < nNodes; i++) {
				try {
					futuresRandom[i].get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			/**
			 * **********************************************************************
			 */
			Thread.sleep(1000);
			System.out.println("\n\n\nBeginning test of random, concurrent, any-to-any communication pattern");
			Thread.sleep(1000);
			/**
			 * **********************************************************************
			 */
			int load = nNodes * 60;
			ScheduledFuture<?>[] futures = new ScheduledFuture[load];
			for (int i = 0; i < load; i++) {
				int k = (int) (Math.random() * nNodes);
				if (k >= nNodes) {
					k = nNodes - 1;
				}
				int j = (int) (Math.random() * nNodes);
				long millis = (long) (Math.random() * 1000);
				if (i % 100 == 0) {
					j = nNodes; // Periodically try sending to a non-existent node
				}
				TX task = new TX(k, j, niots);
				System.out.println("Scheduling random message " + i + " from " +
						k + " to " + j);
				futures[i] =
						(ScheduledFuture<?>) execpool.schedule(task, millis,
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

			/**
			 * **********************************************************************
			 */
			Thread.sleep(4000);
			System.out.println("\n\n\nPrinting overall stats. Number of exceptions =  " +
					numExceptions);
			System.out.println("NIO " + (new NIOInstrumenter()));
			for (NIOTransport<?> niot : niots) {
				niot.stop();
			}
			concurrentSender.stop();
			execpool.shutdown();

			System.out.println("\nTesting notes: The number of missing-or-batched messages should be small. The number of\n"
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
