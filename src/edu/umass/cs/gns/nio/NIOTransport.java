package edu.umass.cs.gns.nio;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.umass.cs.gns.main.GNS;

/**
@author V. Arun
 */

/* This class is designed for NIO transport. The nice thing about this
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
 *  To enable non-blocking connects, the application threads queue connect
 *  events in the synchronized structure pendingConnects. The selector 
 *  thread reads from pendingConnects and sets selection ops to wait 
 *  for a connect event as needed.
 *  
 *  A map SockAddrToSockChannel keeps track of the current socket channel
 *  being used to send data to a given InetSocketAddress. Note that this 
 *  mapping can change if connections fail and are re-established. A failed
 *  connection can get re-established on demand by an application thread 
 *  or by the selector thread when it tries to actually write the data to 
 *  a socket channel and encounters an exception.
 *  
 */
public class NIOTransport implements Runnable {

	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	private Selector selector;

	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(8192);

	protected DataProcessingWorker worker;

	// A list of pending connect calls on which finishConnect needs to be called.
	private LinkedList<ChangeRequest> pendingConnects = null;

	/* The key is a socket address and the value is
	 * a list of messages to be sent to that socket address.
	 */
	private HashMap<InetSocketAddress,ArrayList<ByteBuffer>> pendingWrites=null;

	/* Maps a socket address to a socket channel. The latter may change in case
	 * a connection breaks and a new one needs to be initiated.
	 */
	private HashMap<InetSocketAddress,SocketChannel> SockAddrToSockChannel=null;

	// Maps id to socket address
	private NodeConfig nodeConfig=null;

	/* An id corresponds to a socket address as specified in NodeConfig */
	int myID=-1;

	/* A simple class used to instrument send/receive stats */
	private static NIOInstrumenter nioI=new NIOInstrumenter();

	private Logger log = Logger.getLogger(NIOTransport.class.getName()); //GNS.getLogger();

	public NIOTransport(int id, NodeConfig nc, DataProcessingWorker worker) throws IOException {
		//this.hostAddress = hostAddress;
		//this.port = port;
		this.myID=id;
		this.nodeConfig = nc;
		this.selector = this.initSelector();
		this.worker = worker;

		this.pendingConnects = new LinkedList<ChangeRequest>();
		this.pendingWrites = new HashMap<InetSocketAddress,ArrayList<ByteBuffer>>();
		this.SockAddrToSockChannel = new HashMap<InetSocketAddress,SocketChannel>();
	}

	/* send() methods are called by external application threads. They may
	 * initiate a connection if one is not available. However, the connection
	 * is finished in a non-blocking manner by the selector thread. Data to be
	 * sent is queued in pendingWrites, which is read later by the selector thread.
	 */
	public void send(int id, byte[] data) throws IOException {
		log.finest("Node " + myID + " invoked send (" + id + ", " + 
				new String(data) + "), connection status: " + this.isConnected(id));
		send(new InetSocketAddress(this.nodeConfig.getNodeAddress(id), 
				this.nodeConfig.getNodePort(id)), data);
	}

	public void send(InetSocketAddress isa, byte[] data) throws IOException {
		testAndIntiateConnection(isa);
		nioI.incrSent();
		this.queuePendingWrite(isa, data);
		// Finally, wake up our selecting thread so it can make the required changes
		this.selector.wakeup();
	}

	public void run() {
		while (true) {
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
				e.printStackTrace();
			}
		}
	}

	public static String getStats() {return nioI.toString();}


	/************ Start of private methods ****************************/

	// Invoked only by the selector thread. Typical nio event handling code. 
	private void processSelectedKeys() throws IOException {
		// Iterate over the set of keys for which events are available
		Iterator<SelectionKey> selectedKeys = this.selector.selectedKeys().iterator();
		while (selectedKeys.hasNext()) {
			SelectionKey key = (SelectionKey) selectedKeys.next();
			selectedKeys.remove();

			if (!key.isValid()) continue;
			// Check what event is available and deal with it
			if (key.isAcceptable()) this.accept(key);
			else if (key.isConnectable()) this.finishConnection(key);
			else if (key.isReadable()) this.read(key);
			else if (key.isWritable()) this.write(key);
		}		
	}

	/* Invoked only by the selector thread.
	 * accept immediately sets the channel in read mode as a connection
	 * is presumably being established to send some data. We could also
	 * insert a mapping into SockAddrToSockChannel, but this is unlikely
	 * to be useful as responses will be addressed to the destination's
	 * InetSocketAddress, which would not be the same as the remote 
	 * address of the socket channel just established. 
	 * 
	 */
	private void accept(SelectionKey key) throws IOException {
		// For an accept to be pending the channel must be a server socket channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		log.finer("Accepted");
		socketChannel.configureBlocking(false);
		socketChannel.socket().setKeepAlive(true);
		//this.testAndPutSockAddrToSockChannel((InetSocketAddress)socketChannel.getRemoteAddress(), socketChannel); // synchronized

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ);
	}

	/* Invoked only by the selector thread.
	 * read() is easy as it just needs to read whatever is available
	 * and send it off to DataProcessingWorker (that has to deal with
	 * the complexity of parsing a byte stream).
	 */
	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		assert(socketChannel != null) : "Null socketChannel registered to key " + key;

		// Clear out our read buffer so it's ready for new data
		this.readBuffer.clear();

		// Attempt to read off the channel
		int numRead=-1;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} finally {
			// The remote forcibly or cleanly closed the connection.
			// cancel the selection key and close the channel.
			if(numRead==-1) {
				key.cancel();
				socketChannel.close();  // Will be automatically replaced later if present in SockAddrToSockChannel
				return;
			} else nioI.incrRcvd();
		}

		// Hand the data off to our worker thread
		this.worker.processData(socketChannel, this.readBuffer.array(), numRead);
	}


	/* Invoked only by the selector thread. If a write encounters an exception, 
	 * the selector thread may establish a new connection.
	 */
	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		InetSocketAddress isa = getSockAddrFromSockChannel(socketChannel);
		/* The assertion below is expected to hold because there should be an isa
		 * for each about-to-be-written socket channel. The selector thread
		 * tries to write to a socket channel only if a write op interest
		 * was previously registered. A write op interest is registered
		 * upon either a finishConnect or upon finding an entry in 
		 * pendingWrites. A null isa here can only mean that a write op
		 * interest was registered on some socket channel 
		 * but there is no write in pendingWrites because the corresponding
		 * write was written to a different socket channel. This means that
		 * two connections (or socket channels) were established for the same
		 * isa. That can not happen because testAndInitiateConnection is 
		 * synchronized.
		 */
		assert (isa!=null) : "Node " + myID + " SocketChannel " + socketChannel + " is orphaned with no InetSocketAddress.";
		try {
			// If all data gets written successfully, switch back to read mode.
			if(this.writeAllPendingWrites(isa, socketChannel)) key.interestOps(SelectionKey.OP_READ); // synchronized
		} catch(IOException e) {
			try {
				key.cancel();
				socketChannel.close(); // Will be automatically replaced later if present in SockAddrToSockChannel
			} finally {
				// Note that the cancel or close could again throw an IOException
				this.testAndIntiateConnection(isa);
			}
		}
	}

	/* **************************************************************
	 * Start of methods synchronizing on pendingWrites.
	 ******************************************************************/

	/* Invoked only by the selector thread.
	 * Writes to socket channel and de-queues writes. If an exception occurs,
	 * it lets the bytes remain in the queue. However, we don't know if or how 
	 * many of those bytes got sent to the other side. Basically, socket exceptions
	 * are bad news and will likely result in some missing data.
	 */
	private boolean writeAllPendingWrites(InetSocketAddress isa, SocketChannel socketChannel) throws IOException {
		synchronized (this.pendingWrites) {	
			ArrayList<ByteBuffer> queue = (ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
			// Write until there's not more data ...
			if(queue==null) return true;
			while (!queue.isEmpty()) {
				ByteBuffer buf = (ByteBuffer) queue.get(0);
				socketChannel.write(buf);
				// If the socket's buffer fills up, let the rest be in queue 
				if (buf.remaining() > 0) break;
				queue.remove(0);
			}
			// We wrote away all data, so we're no longer interested in
			// writing on this socket. Switch back to waiting for data.
			if (queue.isEmpty()) return true;
			else return false;
		}
	}

	/* Invoked by application threads so that the selector thread can
	 * process them.
	 */
	private void queuePendingWrite(InetSocketAddress isa, byte[] data) {
		synchronized (this.pendingWrites) {
			ArrayList<ByteBuffer> queue = (ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
			if(queue==null) {
				queue = new ArrayList<ByteBuffer>();
				this.pendingWrites.put(isa, queue);
			}
			queue.add(ByteBuffer.wrap(data));
		}
	}

	/* Invoked only by the selector thread.
	 * Register a write interest on all sockets that have at least one 
	 * pending write. Note that it is not enough to register a write 
	 * interest just once unless you can ensure that the interest will
	 * be converted to a successful write; if not, writes can get missed.
	 */
	private void registerWriteInterests() {
		synchronized (this.pendingWrites) {
			for(InetSocketAddress isa : this.pendingWrites.keySet()) {
				ArrayList<ByteBuffer> queue = (ArrayList<ByteBuffer>) this.pendingWrites.get(isa);
				if(queue!=null && !queue.isEmpty()) {
					// Nested locking: pendingWrites -> SockAddrToSockChannel
					SocketChannel sc = getSockAddrToSockChannel(isa);  // synchronized
					SelectionKey key = (sc!=null ? sc.keyFor(this.selector) : null);
					if(key!=null) key.interestOps(SelectionKey.OP_WRITE);
				}
			}
		}
	}
	/* **************************************************************
	 * End of methods synchronizing on pendingWrites.
	 ******************************************************************/

	/* **************************************************************
	 * Start of methods synchronizing on SockAddrToSockChannel.
	 ******************************************************************/

	/* Reverse lookup. Should implement a bidirectional hashmap for 
	 * more efficiency.
	 */
	private InetSocketAddress getSockAddrFromSockChannel(SocketChannel sc) {
		synchronized (this.SockAddrToSockChannel) {
			InetSocketAddress retval=null;
			if(this.SockAddrToSockChannel.containsValue(sc)) {
				for(InetSocketAddress isa : this.SockAddrToSockChannel.keySet()) {
					if(this.SockAddrToSockChannel.get(isa).equals(sc)  || this.SockAddrToSockChannel.get(isa)==sc) retval = isa;
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
	private void putSockAddrToSockChannel(InetSocketAddress isa, SocketChannel socketChannel) {
		synchronized (this.SockAddrToSockChannel) {
			log.finest("Node " + myID + " inserting (" + isa + ", " + socketChannel + ")");
			this.SockAddrToSockChannel.put(isa, socketChannel); 
		}
	}
	private boolean isConnected(InetSocketAddress isa) {
		synchronized(this.SockAddrToSockChannel) {
			SocketChannel sock = (SocketChannel)this.SockAddrToSockChannel.get(isa);
			if(sock!=null && (sock.isConnected() || sock.isConnectionPending())) return true;
			log.finest("Node " + myID + " socket channel [" + sock + "] not connected");
			return false;
		}
	}

	/* Initiate a connection if the existing socket channel is not connected.
	 * Synchronization ensures that the test and connect happen atomically.
	 * If not, we can have additional unused sockets accumulated that can
	 * cause memory leaks over time.
	 */
	private SocketChannel testAndIntiateConnection(InetSocketAddress isa) throws IOException {
		synchronized(this.SockAddrToSockChannel) {
			SocketChannel sock=null;
			if(!this.isConnected(isa)) {
				SocketChannel oldSock = this.getSockAddrToSockChannel(isa); // synchronized
				if(oldSock!=null) {log.severe("SocketChannel " + oldSock + " found dead. " + 
						"Could be because the remote end closed the connection");}
				sock = this.initiateConnection(isa);
			}
			return sock;
		}
	}


	/* **************************************************************
	 * End of methods synchronizing on SockAddrToSockChannel.
	 ******************************************************************/

	/* **************************************************************
	 * Start of methods synchronizing on pendingConnects.
	 ******************************************************************/
	private void addPendingConnect(SocketChannel socketChannel) {
		synchronized(this.pendingConnects) {
			this.pendingConnects.add(new ChangeRequest(socketChannel, 
					ChangeRequest.REGISTER, SelectionKey.OP_CONNECT));
		}
	}
	/* Process any pending connect requests to ensure that when the socket
	 * is connectable, finishConnect is called.
	 */
	private void processPendingConnects() throws ClosedChannelException{
		synchronized (this.pendingConnects) {
			Iterator<ChangeRequest> changes = this.pendingConnects.iterator();
			while (changes.hasNext()) {
				ChangeRequest change = (ChangeRequest) changes.next();
				log.finest("Node " + myID + " processing connect event : " + change);
				switch (change.type) {
				case ChangeRequest.CHANGEOPS:
					SelectionKey key = change.socket.keyFor(this.selector);
					key.interestOps(change.ops);
					break;
				case ChangeRequest.REGISTER:
					change.socket.register(this.selector, change.ops);
					break;
				}
			}
			this.pendingConnects.clear();
		}
	}

	/* **************************************************************
	 * End of methods synchronizing on pendingConnects.
	 ******************************************************************/


	private Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		// Bind the server socket to the specified address and port
		log.finest("Node " + myID + " listening on " + this.nodeConfig.getNodeAddress(this.myID) + 
				":"+ this.nodeConfig.getNodePort(this.myID));
		InetSocketAddress isa = new InetSocketAddress(this.nodeConfig.getNodeAddress(this.myID), 
				this.nodeConfig.getNodePort(this.myID));
		serverChannel.socket().bind(isa);

		// Register the server socket channel, indicating an interest in 
		// accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}

	private boolean isConnected(int id) {
		return isConnected(new InetSocketAddress(this.nodeConfig.getNodeAddress(id), 
				this.nodeConfig.getNodePort(id)));
	}

	/* This method will definitely initiate a new connection and replace existing 
	 * entries in SockAddrToSockChannel. It is the responsibility of the caller 
	 * to check if a connection to isa already exists.
	 */
	private SocketChannel initiateConnection(InetSocketAddress isa) throws IOException {
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);

		// Kick off connection establishment
		log.finest("Node " + myID + " connecting to socket address " + isa);		
		socketChannel.connect(isa);
		putSockAddrToSockChannel(isa, socketChannel); // synchronized

		// Queue a channel registration since the caller is not the 
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		addPendingConnect(socketChannel); // synchronized

		return socketChannel;
	}


	/* Invoked only by the selector thread.
	 * Sets selection ops to write as the connection is presumably being 
	 * set up to write something.
	 */
	private boolean finishConnection(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		boolean connected = false;

		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			connected = socketChannel.finishConnect();
		} catch (IOException e) {
			// Cancel the channel's registration with our selector
			e.printStackTrace();
			key.cancel();
		}

		/* Register an interest in writing on this channel. No 
		 * point registering a write interest until the socket
		 * is connected as the write will block anyway.
		 */
		if(connected) log.finest("Node " + myID + " finished connecting " + socketChannel);
		if(connected) key.interestOps(SelectionKey.OP_WRITE);
		return connected;
	}

	public static void main(String[] args) {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.FINEST);
		Logger log = Logger.getLogger(NIOTransport.class.getName()); 
		log.addHandler(handler);
		log.setLevel(Level.FINEST);

		int port = 2000;
		int nNodes=100;
		SampleNodeConfig snc = new SampleNodeConfig(port);
		snc.localSetup(nNodes);
		//System.out.println(snc);
		DefaultDataProcessingWorker worker = new DefaultDataProcessingWorker();
		NIOTransport[] niots = new NIOTransport[nNodes];

		try {
			int smallNNodes = 2;
			for(int i=0; i<smallNNodes; i++) {
				niots[i] = new NIOTransport(i, snc, worker);
				new Thread(niots[i]).start();
			}			

			/*************************************************************************/
			/* Test a few simple hellos. The sleep is there to test 
			 * that the successive writes do not "accidentally" benefit
			 * from concurrency, i.e., to check that write ops flags will
			 * be set correctly.
			 */
			niots[1].send(0, "Hello from 1 to 0".getBytes());
			niots[0].send(1, "Hello back from 0 to 1".getBytes());
			niots[0].send(1, "Second hello back from 0 to 1".getBytes());
			try {Thread.sleep(1000);} catch(Exception e){e.printStackTrace();}
			niots[0].send(1, "Third hello back from 0 to 1".getBytes());
			niots[1].send(0, "Thank you for all the hellos back from 1 to 0".getBytes());
			/*************************************************************************/

			Thread.sleep(2000);
			System.out.println("\n\n\nBeginning test of random, sequential communication pattern");
			Thread.sleep(1000);

			/*************************************************************************/
			//Create the remaining nodes up to nNodes
			for(int i=smallNNodes; i<nNodes; i++) {
				niots[i] = new NIOTransport(i, snc, worker);
				new Thread(niots[i]).start();
			}			

			// Test a random, sequential communication pattern
			for(int i=0; i<nNodes; i++) {
				int k = (int)(Math.random()*nNodes);
				int j = (int)(Math.random()*nNodes);
				System.out.println("Message " + i);
				niots[k].send(j, ("Hello from " + k + " to " + j).getBytes());
			}
			/*************************************************************************/
			Thread.sleep(1000);
			System.out.println("\n\n\nBeginning test of random, concurrent communication pattern");
			Thread.sleep(1000);
			/*************************************************************************/
			// Test a random, concurrent communication pattern
			Timer T = new Timer();
			class TX extends TimerTask {
				private int sndr=-1;
				private int rcvr=-1;
				private NIOTransport[] niots=null;
				TX(int i, int j, NIOTransport[] n) {
					sndr = i;
					rcvr = j;
					niots = n;
				}
				public void run() {
					try {
						niots[sndr].send(rcvr, ("Hello from " + sndr + " to " + rcvr).getBytes());
					} catch(IOException e) {
						e.printStackTrace();
					}
				}
			}
			for(int i=0; i<nNodes; i++) {
				int k = (int)(Math.random()*nNodes);
				int j = (int)(Math.random()*nNodes);
				long millis = (long)(Math.random()*1000);
				TX task = new TX(k, j, niots);
				System.out.println("Scheduling message " + i);
				T.schedule(task, millis);
			}

			/*************************************************************************/

			Thread.sleep(1000);
			System.out.println("\n\n\nPrinting overall stats");
			Thread.sleep(1000);
			System.out.println(NIOTransport.getStats());

			System.out.println("\nTesting notes: If no exceptions were encountered above and\n" +
					" the number of MISSING messages is small, then it is a successful test. If this\n" + 
					" note is being printed, then no exceptions were encountered. The number of\n" + 
					" MISSING messages may still be nonzero as two back-to-back messages may get\n" +
					" counted as 1, as in the first test above. The other tests are\n" +
					" extremely unlikely to cause such bunching, so MISSING should be at most 1\n" +
					" with the above tests");
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
