package edu.umass.cs.gns.nio;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 6/29/13
 * Time: 6:34 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//import edu.umass.cs.gnrs.main.GNS.PortType;
//import edu.umass.cs.gnrs.packet.Packet;
//import edu.umass.cs.gnrs.util.ConfigFileInfo;

public class NioServer2 implements Runnable {

  // The host:port combination to listen on
  static final int MAX_QUEUE_DISCONNECTED = 10000;
  private int ID;
  private InetAddress myAddress;
  private int myPort;
  // The channel on which we'll accept connections
  private ServerSocketChannel serverChannel;
  // The selector we'll be monitoring
  private Selector selector;
  // The buffer into which we'll read data when it's available
  private ByteBuffer readBuffer = ByteBuffer.allocate(8192);
  private ByteStreamToJSONObjects workerObject;
  // A list of PendingChange instances
  private List pendingChanges = new LinkedList();
  // Maps a SocketChannel to a list of ByteBuffer instances
  private Map pendingData = new HashMap();
  private boolean newPendingData = false;
  private boolean[] pendingChangeByNode;
  // ABHIGYAN
  // Used only for sending not for receiving.
  private SocketChannel[] connectedIDs; // K = ID, V = SocketChannel
  private Map connectionAttempts = new ConcurrentHashMap(); // K = ID, V = number of attempts
  private static int MAX_ATTEMPTS = 20; // max attempts at connection;
  private int numberOfConnectionsInitiated = 0;
  Timer t = new Timer();
  //    private HashMap<Integer, InetAddress> IDToIPMappings;
  private NodeConfig nodeConfig;

  public NioServer2(int ID, ByteStreamToJSONObjects worker, NodeConfig nodeConfig) throws IOException {
    connectedIDs = new SocketChannel[nodeConfig.getNodeCount()];
    pendingChangeByNode = new boolean[nodeConfig.getNodeCount()];
    for (int i = 0; i < pendingChangeByNode.length; i++)
      pendingChangeByNode[i] = false;
    this.ID = ID;
    this.myAddress = nodeConfig.getNodeAddress(ID);
    this.myPort = nodeConfig.getNodePort(ID);
    this.selector = this.initSelector();
    this.workerObject = worker;
    this.nodeConfig = nodeConfig;
    t.schedule(new WakeupSelectorTask(this),1,1);
  }

  void wakeupSelector() {
    boolean wakeup;
    synchronized (connectedIDs) {
      synchronized (pendingChanges) {
        synchronized (pendingData) {
          wakeup = newPendingData;
          newPendingData = false;

          for (int i = 0; i < pendingChangeByNode.length ; i++) {
            if (pendingChangeByNode[i]) {
              this.pendingChanges.add(new ChangeRequest(connectedIDs[i], ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
              pendingChangeByNode[i] = false;
            }
          }
        }
      }
    }
    if (wakeup) selector.wakeup();
  }


//    public void sendToAll(JSONObject json, Set<Integer> destIDs, Set<Integer> ports, Set<Integer> excludeNameServerIds) throws JSONException, IOException {
//
//        for (Integer id : destIDs) {
//            if (excludeNameServerIds.contains(id)) {
//                continue;
//            }
//            int destPort = Packet.getPort(id, portType);
//            sendToID(id, destPort, json);
//        }
//    }
//
//    public void sendToAll(JSONObject json, Set<Integer> destIDs,
//                          GNS.PortType portType, int excludeNameServerId) throws JSONException, IOException {
//
//        for (Integer id : destIDs) {
//            if (id.intValue() == excludeNameServerId) {
//                continue;
//            }
//            int destPort = Packet.getPort(id, portType);
//            sendToID(id, destPort, json);
//        }
//    }
//
//    public void sendToAll(JSONObject json, Set<Integer> destIDs, PortType portType)
//            throws IOException, JSONException {
//        for (Integer destID : destIDs) {
//            int destPort = Packet.getPort(destID, portType);
//            sendToID(destID, destPort, json);
//        }
//    }

  //    /**
//     * Send to
//     *
//     * @param destID at port type
//     * @param portType
//     * @param destID
//     * @param portType
//     * @param json
//     * @return
//     * @throws IOException
//     * @throws JSONException
//     */
//    public boolean sendToID(JSONObject json, int destID, PortType portType) throws IOException, JSONException {
//        int destPort = Packet.getPort(destID, portType);
//        return sendToID(destID, destPort, json);
//    }

  public void sendToIDs(Set<Integer> destIDs, JSONObject json) throws IOException {
    sendToIDs(destIDs, json, -1);
  }

  public void sendToIDs(Set<Integer> destIDs, JSONObject json, int excludeID) throws IOException {
    if (destIDs.contains(ID) && (excludeID != ID)) { // to send to same node, directly call the demultiplexer
      ArrayList e = new ArrayList();
      try {
        e.add(new JSONObject(json.toString()));
      } catch (JSONException e1) {
        e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      workerObject.getPacketDemux().handleJSONObjects(e);
    }
//        System.out.println(" Here in sendtoIDs");
    String s = json.toString();
    byte[] data = ("&" + s.length() + "&" + s).getBytes();
    boolean okay = true;
    synchronized (this.connectedIDs) {
//            SocketChannel socketChannel = null;
      for (int destID: destIDs) {
        if (destID == excludeID) continue;
        if (destID == ID) continue;
        if (connectedIDs[destID] != null && connectedIDs[destID].isConnected()) continue;
        okay = false;
        break;
      }
//            System.out.println(" Status of okay " + okay);
      if (okay) {
//                synchronized (this.pendingChanges) {
        synchronized (this.pendingData) {
          for (int destID: destIDs) {
            if (destID == excludeID) continue;
//                            System.out.println(" Sending to " + destID);
            if (destID == ID) continue;
            // Indicate we want the interest ops set changed
//                            if (pendingChangeByNode[destID] == false) {
//                                this.pendingChanges.add(new ChangeRequest(connectedIDs[destID], ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
//                                pendingChangeByNode[destID] = true;
//                            }
            newPendingData = true;
            pendingChangeByNode[destID] = true;
            // And queue the data we want written
            List queue = (List) this.pendingData.get(connectedIDs[destID]);
            if (queue == null) {
              queue = new ArrayList();
              this.pendingData.put(connectedIDs[destID], queue);
            }


            queue.add(ByteBuffer.wrap(data));

          }
//                    }
        }
//                selector.wakeup();
      }
    }

    if (!okay) {
      for (int destID:destIDs) {
        if (destID == ID || destID == excludeID) continue;
        sendToID(destID, json);
      }
    }
  }

  public boolean sendToID(int destID, JSONObject json) throws IOException {

    if (destID == ID) { // to send to same node, directly call the demultiplexer
      ArrayList e = new ArrayList();
      e.add(json);
      workerObject.getPacketDemux().handleJSONObjects(e);
      return true;
    }

//        if (!nodeConfig.containsNodeInfo(destID)) {
//            if (StartNameServer.debugMode) GNS.getLogger().severe("ERROR: Message Not Sent. Node Config does " +
//                    "not contain info on nodeID = " + destID);
//            return false;
//        }

    // append a packet length header to JSON object

    String s = json.toString();
    byte[] data = ("&" + s.length() + "&" + s).getBytes();

    if (!nodeConfig.containsNodeInfo(destID)) return false;

//        SocketChannel socketChannel = null;
    // synchronized for thread safety
    synchronized (this.connectedIDs) {
//      if (connectionAttempts.containsKey(destID)) {// && (Integer) connectionAttempts.get(destID) >= MAX_ATTEMPTS) {
//        if (StartNameServer.debugMode) GNRS.getLogger().severe("NIOEXCEPTION: Could not connect. Max attempts reached. = " + MAX_ATTEMPTS);
//        return false;
//      }
      if (connectedIDs[destID] != null && connectedIDs[destID].isConnected()) { // connected
        send(destID, connectedIDs[destID], data);
//                connectionAttempts.put(destID, 0);

      } else if (connectedIDs[destID] != null && connectedIDs[destID].isConnectionPending()) { // add to pending data
//                connectionAttempts.put(destID, 0);
        synchronized (this.pendingData) {
          List queue = (List) this.pendingData.get(connectedIDs[destID]);
          if (queue == null) {
            queue = new ArrayList();
            this.pendingData.put(connectedIDs[destID], queue);
          }
          else if (queue.size() > MAX_QUEUE_DISCONNECTED) {
            queue.remove(0);
          }
          queue.add(ByteBuffer.wrap(data));
        }

      } else {
        numberOfConnectionsInitiated++;
//                if (StartNameServer.debugMode) GNS.getLogger().info("\tNIOSTAT\tconnection-event\t"
//                        + numberOfConnectionsInitiated + "\t" + destID + "\t");
        // new connection.
//				String host = ((String)ID_to_IP_Port.get(id)).split(":")[0];
//				int port = Integer.parseInt(((String)ID_to_IP_Port.get(id)).split(":")[1]);
        SocketChannel newSocketChannel = SocketChannel.open();
        newSocketChannel.configureBlocking(false);
        InetAddress address = nodeConfig.getNodeAddress(destID);
        int destPort = nodeConfig.getNodePort(destID);

        // Kick off connection establishment
        newSocketChannel.connect(new InetSocketAddress(address, destPort));

//        SocketChannel newSocketChannel = this.initiateConnection(address, destPort);
        SocketChannel socketChannel = connectedIDs[destID];
        connectedIDs[destID] = newSocketChannel;

        synchronized (this.pendingData) {
          // read old entries.
          List queue = null;
          if (this.pendingData.containsKey(socketChannel)) {
            queue = (List) this.pendingData.remove(socketChannel);
            if (queue.size() > MAX_QUEUE_DISCONNECTED) {
              queue.remove(0);
            }
          }
          if (queue == null) {
            queue = new ArrayList();
          }

          queue.add(ByteBuffer.wrap(data));
          this.pendingData.put(newSocketChannel, queue);
          this.pendingChangeByNode[destID] = false;
//                    newPendingData = true;
        }

        // Queue a channel registration since the caller is not the
        // selecting thread. As part of the registration we'll register
        // an interest in connection events. These are raised when a channel
        // is ready to complete connection establishment.
        synchronized (this.pendingChanges) {
          this.pendingChanges.add(new ChangeRequest(newSocketChannel, ChangeRequest.REGISTER, SelectionKey.OP_CONNECT));

        }

        this.selector.wakeup();
      }
    }

    return true;
  }

//  // ABHIGYAN
//  private SocketChannel initiateConnection(InetAddress address, int port) throws IOException {
//
//    // Create a non-blocking socket channel
//    SocketChannel socketChannel = SocketChannel.open();
//    socketChannel.configureBlocking(false);
//
//    // Kick off connection establishment
//    socketChannel.connect(new InetSocketAddress(address, port));
//
//    // Queue a channel registration since the caller is not the
//    // selecting thread. As part of the registration we'll register
//    // an interest in connection events. These are raised when a channel
//    // is ready to complete connection establishment.
//    synchronized (this.pendingChanges) {
//      this.pendingChanges.add(new ChangeRequest(socketChannel, ChangeRequest.REGISTER, SelectionKey.OP_CONNECT));
//    }
//    return socketChannel;
//  }

  private void send(int x, SocketChannel socket, byte[] data) {

//        synchronized (this.pendingChanges) {
    // Indicate we want the interest ops set changed
//            if (pendingChangeByNode[x] == false) {
//                this.pendingChanges.add(new ChangeRequest(socket, ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
//                this.pendingChangeByNode[x] = true;
//            }

    // And queue the data we want written
    synchronized (this.pendingData) {
      List queue = (List) this.pendingData.get(socket);
      if (queue == null) {
        queue = new ArrayList();
        this.pendingData.put(socket, queue);
      }
      queue.add(ByteBuffer.wrap(data));
      newPendingData = true;
      this.pendingChangeByNode[x] = true;
    }
//        }

    // Finally, wake up our selecting thread so it can make the required changes
//        this.selector.wakeup();
  }

  public void run() {
    while (true) {
      try {
        // Process any pending changes
        synchronized (this.pendingChanges) {
          Iterator changes = this.pendingChanges.iterator();
          while (changes.hasNext()) {
            ChangeRequest change = (ChangeRequest) changes.next();
            switch (change.type) {
              case ChangeRequest.CHANGEOPS:
                SelectionKey key = change.socket.keyFor(this.selector);
                if (key != null && key.isValid()) {
                  key.interestOps(change.ops);
                } else {
                  if (StartNameServer.debugMode) GNS.getLogger().severe("INVALID KEY: ");
                }
                break;
              case ChangeRequest.REGISTER:
//                                if (StartNameServer.debugMode) GNS.getLogger().info("SELECTOR: Socket registered with this selector.");
                change.socket.register(this.selector, change.ops);
                break;
            }
          }
          this.pendingChanges.clear();
        }

        // Wait for an event one of the registered channels
//                if (StartNameServer.debugMode) GNS.getLogger().finer("SELECTOR: blocking.");
        this.selector.select();

        // Iterate over the set of keys for which events are available
        Iterator selectedKeys = this.selector.selectedKeys().iterator();
        while (selectedKeys.hasNext()) {
          SelectionKey key = (SelectionKey) selectedKeys.next();
          selectedKeys.remove();

          if (!key.isValid()) {
            continue;
          }

          // Check what event is available and deal with it
          // ABHIGYAN
          if (key.isConnectable()) {
//                        if (StartNameServer.debugMode) GNS.getLogger().finer("SELECTOR Key: connectable.");
            this.finishConnection(key);
          } else if (key.isAcceptable()) {
//                        if (StartNameServer.debugMode) GNS.getLogger().finer("SELECTOR Key: acceptable.");
            this.accept(key);
          } else if (key.isReadable()) {
//                        if (StartNameServer.debugMode) GNS.getLogger().finer("SELECTOR Key: readable.");
            this.read(key);
          } else if (key.isWritable()) {
//                        if (StartNameServer.debugMode) GNS.getLogger().finer("SELECTOR Key: writable.");
            this.write(key);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  // ABHIGYAN
  private void finishConnection(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();


    synchronized(this.connectedIDs) {
      // Finish the connection. If the connection operation failed
      // this will raise an IOException.
      try {
        socketChannel.finishConnect();
        socketChannel.socket().setKeepAlive(true);
      } catch (IOException e) {
        // Cancel the channel's registration with our selector
        if (StartNameServer.debugMode) GNS.getLogger().severe(e.getMessage());
        key.cancel();
        return;
      }
    }
    // Register an interest in writing on this channel
    key.interestOps(SelectionKey.OP_WRITE);

  }

  private void accept(SelectionKey key) throws IOException {
    // For an accept to be pending the channel must be a server socket channel.
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

    // Accept the connection and make it non-blocking
    SocketChannel socketChannel = serverSocketChannel.accept();
    Socket socket = socketChannel.socket();
    // ABHIGYAN:
    socketChannel.socket().setKeepAlive(true);
    socketChannel.configureBlocking(false);
    // lookup ID for this hostIP and port.
//		includeSocketChannelInConnectedList(socketChannel);

    // Register the new SocketChannel with our Selector, indicating
    // we'd like to be notified when there's data waiting to be read
    socketChannel.register(this.selector, SelectionKey.OP_READ);
  }

  private void read(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();

    // Clear out our read buffer so it's ready for new data
    this.readBuffer.clear();
    int numRead = 0;
//        synchronized(this.connectedIDs) {
    // Attempt to read off the channel

    try {
      numRead = socketChannel.read(this.readBuffer);
    } catch (IOException e) {
      // The remote forcibly closed the connection, cancel
      // the selection key and close the channel.
      if (StartNameServer.debugMode) GNS.getLogger().severe("READ EXCEPTION, FORCED CLOSE CONNECTION.");
      key.cancel();
      socketChannel.close();
      return;
    }

    if (numRead == -1) {
      // Remote entity shut the socket down cleanly. Do the
      // same from our end and cancel the channel.
      if (StartNameServer.debugMode) GNS.getLogger().severe("REMOTE ENTITY SHUT DOWN SOCKET CLEANLY.");
      key.channel().close();
      key.cancel();
      //
      return;
    }
//        }
    // Hand the data off to the worker thread
    this.workerObject.processData(socketChannel, this.readBuffer.array(), numRead);
  }

  private void write(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();

    synchronized (this.pendingData) {
      List queue = (List) this.pendingData.get(socketChannel);

      // Write until there's not more data ...
      while (!queue.isEmpty()) {
        ByteBuffer buf = (ByteBuffer) queue.get(0);
        socketChannel.write(buf);
        if (buf.remaining() > 0) {
          // ... or the socket's buffer fills up
          break;
        }
        queue.remove(0);
      }

      if (queue.isEmpty()) {
        // We wrote away all data, so we're no longer interested
        // in writing on this socket. Switch back to waiting for
        // data.
        key.interestOps(SelectionKey.OP_READ);
      }
    }
  }

  private Selector initSelector() throws IOException {
    // Create a new selector
    Selector socketSelector = SelectorProvider.provider().openSelector();

    // Create a new non-blocking server socket channel
    this.serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);

    // Bind the server socket to the specified address and port
    InetSocketAddress isa = new InetSocketAddress(this.myAddress, this.myPort);
    serverChannel.socket().bind(isa);

    // Register the server socket channel, indicating an interest in
    // accepting new connections
    serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

    return socketSelector;
  }

  public static void main(String[] args) {
    int ID = Integer.parseInt(args[0]);
    int port = 9000 + 10 * ID;
    try {
      ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(null);
      new Thread(worker).start();
      NioServer server = new NioServer(ID, null, port, worker);
      new Thread(server).start();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      int count = 0;

      while (count < 100) {
        count++;
        System.out.println("COUNT " + count);

        int sendTo = (ID + 1) % 2;

        if (sendTo != ID && sendTo >= 0) // TODO : Fix this to test this method.
        //					server.sendToID(sendTo, ("\t\t\tID " + ID +" Send to ID "  + sendTo + "\n").getBytes());
        {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

class WakeupSelectorTask extends TimerTask {
  NioServer2 nioServer2;
  public WakeupSelectorTask(NioServer2 nioServer2) {
    this.nioServer2 = nioServer2;
  }
  @Override
  public void run() {
    nioServer2.wakeupSelector();
  }
}
//private Map connectionStatus = new ConcurrentHashMap(); // 0 = not connected, 1 = connection initiated, 2 = connected.
//
//
//// ABHIGYAN
//public void sendToID(int id, byte[] data) throws IOException {
//
//	synchronized (this.connectionStatus) {
//		int status = 0;
//		if (connectionStatus.containsKey(id)) status = (Integer) connectionStatus.get(id);
//
//		// ID is connected
//		if (status == 2) {
//			// then connectedIDs must also contain a SocketChannel object.
//			if (connectedIDs.containsKey(id)) {
//				SocketChannel socketChannel = (SocketChannel) connectedIDs.get(id);
//				send(socketChannel, data);
//			}
//		} else if (status == 1) { // connected initiated.
//			// get socketChannel
//			SocketChannel socketChannel = (SocketChannel) connectedIDs.get(id);
//			// Add to pendingData
//			synchronized (this.pendingData) {
//				List queue = (List) this.pendingData.get(socketChannel);
//				if (queue == null) {
//					queue = new ArrayList();
//					this.pendingData.put(socketChannel, queue);
//				}
//				queue.add(ByteBuffer.wrap(data));
//			}
//		} else if (status == 0) { // establish connection and send.
//			System.out.println("SendToID:" + status);
//			String host = ((String)ID_to_IP_Port.get(id)).split(":")[0];
//			int port = Integer.parseInt(((String)ID_to_IP_Port.get(id)).split(":")[1]);
//
//			SocketChannel socketChannel = this.initiateConnection(host, port);
//			connectedIDs.put(id, socketChannel);
//			connectionStatus.put(id, 1); // change status to connection initiated.
//
//			// ABHIGYAN
//			synchronized (this.pendingData) {
//				List queue = (List) this.pendingData.get(socketChannel);
//				if (queue == null) {
//					queue = new ArrayList();
//					this.pendingData.put(socketChannel, queue);
//				}
//				queue.add(ByteBuffer.wrap(data));
//			}
//			this.selector.wakeup();
//		} else {
//			System.out.println("ERROR: invalid status in connection status");
//		}
//	}
//
//}
//
//	private void includeSocketChannelInConnectedList(SocketChannel socketChannel) throws SocketException {
//		synchronized (this.connectionStatus) {
////			String hostIP = socketChannel.socket().getInetAddress().getHostAddress();
//			String hostIP  = "127.0.0.1";
//			int port = socketChannel.socket().getPort();
//			if (IP_Port_to_ID.containsKey(hostIP + ":" + port)) {
//				int id = (Integer) IP_Port_to_ID.get(hostIP + ":" + port);
//				System.out.println("CONNECTED to " + id);
//			}
//		}
//	}
//