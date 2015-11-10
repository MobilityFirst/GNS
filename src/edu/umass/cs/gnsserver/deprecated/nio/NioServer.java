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
 */
package edu.umass.cs.gnsserver.deprecated.nio;

/* This class is deprecated. The plan is to move to GNSNIOTransport instead. */
// When will this move take place? 
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES;
import edu.umass.cs.nio.interfaces.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;

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

@Deprecated
@SuppressWarnings("unchecked")
public class NioServer<NodeIDType> implements Runnable, JSONNIOTransport<NodeIDType> {

  public static String Version = "$Revision: 838 $";
  public static final boolean DEBUG = false;
  
  // The host:port combination to listen on
  private NodeIDType ID;
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
  private  Map<NodeIDType, Boolean> pendingChangeByNode;
  //private boolean[] pendingChangeByNode;
  // ABHIGYAN
  // Used only for sending not for receiving.
  private Map<NodeIDType, SocketChannel> connectedIDs;
  //private SocketChannel[] connectedIDs; // K = ID, V = SocketChannel
  //private Map connectionAttempts = new ConcurrentHashMap(); // K = ID, V = number of attempts
  private static int MAX_ATTEMPTS = 20; // max attempts at connection;
  private int numberOfConnectionsInitiated = 0;
  Timer t = new Timer();
  //    private HashMap<Integer, InetAddress> IDToIPMappings;
  private NodeConfig nodeConfig;

  private boolean emulateDelay = false;
  private double variation = 0.1;
  private GNSNodeConfig<NodeIDType> gnsNodeConfig = null;

  public NioServer(NodeIDType ID, ByteStreamToJSONObjects worker, NodeConfig nodeConfig) throws IOException {
    
    connectedIDs = new HashMap<NodeIDType, SocketChannel>();
    //connectedIDs = new SocketChannel[nodeConfig.getNodeIDs().size()];
    pendingChangeByNode = new HashMap<NodeIDType, Boolean>();
    //pendingChangeByNode = new boolean[nodeConfig.getNodeIDs().size()];
//    for (int i = 0; i < nodeConfig.getNodeIDs().size(); i++) {
//      pendingChangeByNode[i] = false;
//    }
    this.ID = ID;
    this.myAddress = nodeConfig.getNodeAddress(ID);
    this.myPort = nodeConfig.getNodePort(ID);
    while (true) {
      try {
        this.selector = this.initSelector();
        break;
      } catch (IOException e) {
        int t = 1;
        e.printStackTrace();
        GNS.getLogger().severe("Socket bind failed ... trying again in " + t + " seconds .. ");
        try {
          Thread.sleep(t * 1000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
    this.workerObject = worker;
    this.nodeConfig = nodeConfig;
    t.schedule(new WakeupSelectorTask(this), 1, 1);

    GNS.getLogger().info("Node " + this.ID + " starting NioServer Listener on port " + nodeConfig.getNodePort(this.ID));

  }

  @Override
  public NodeIDType getMyID() {
    return this.ID;
  }

  @Override
  public void stop() {

  }

  /**
   * After this method is called, we emulate an additional delay in packets sent to all nodes.
   */
  public void emulateConfigFileDelays(GNSNodeConfig<NodeIDType> gnsNodeConfig, double variation) {
    this.emulateDelay = true;
    this.variation = variation;
    this.gnsNodeConfig = gnsNodeConfig;
  }

  void wakeupSelector() {
    boolean wakeup;
    synchronized (connectedIDs) {
      synchronized (pendingChanges) {
        synchronized (pendingData) {
          wakeup = newPendingData;
          newPendingData = false;

          for (NodeIDType nodeId : gnsNodeConfig.getNodeIDs()) 
            if (pendingChangeByNode.containsKey(nodeId) && pendingChangeByNode.get(nodeId)) {
              this.pendingChanges.add(new ChangeRequest(connectedIDs.get(nodeId), ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
              //this.pendingChanges.add(new ChangeRequest(connectedIDs[i], ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
              pendingChangeByNode.put(nodeId, false);
            }
          }
//          for (int i = 0; i < pendingChangeByNode.length; i++) {
//            if (pendingChangeByNode[i]) {
//              this.pendingChanges.add(new ChangeRequest(connectedIDs.toString(i), ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
//              //this.pendingChanges.add(new ChangeRequest(connectedIDs[i], ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
//              pendingChangeByNode[i] = false;
//            }
//          }
//       }
      }
    }
    if (wakeup) {
      selector.wakeup();
    }
  }

  private Random random = new Random();

  @Override
  public int sendToID(NodeIDType destID, JSONObject json) throws IOException {
    if (emulateDelay) {
      long delay = gnsNodeConfig.getPingLatency(destID) / 2; // divide by 2 for one-way delay
      delay = (long) ((1.0 + this.variation * random.nextDouble()) * delay);
      //    GNS.getLogger().severe("Delaying packet by " + delay + "ms");
      SendQueryWithDelay2 timerObject = new SendQueryWithDelay2(this, destID, json);
      t.schedule(timerObject, delay);
      return 0;
    } else {
      if (DEBUG && Packet.filterOutChattyPackets(json)) {
        GNS.getLogger().info("##### SEND " + Packet.getPacketTypeStringSafe(json) + ""
                + " : From " + ID + " to " + destID + ": " + json.toString());
      }
      sendToIDActual(destID, json);
      return 0;
    }
  }

  @Override
  public int sendToAddress(InetSocketAddress isa, JSONObject jsonData) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  boolean sendToIDActual(NodeIDType destID, JSONObject json) throws IOException {

    if (destID.equals(ID)) { // to send to same node, directly call the demultiplexer
      workerObject.getPacketDemux().handleMessage(json);
      return true;
    }
    // append a packet length header to JSON object
    String s = json.toString();
    byte[] data = ("&" + s.length() + "&" + s).getBytes();

    if (!nodeConfig.nodeExists(destID)) {
      return false;
    }

//        SocketChannel socketChannel = null;
    // synchronized for thread safety
    synchronized (this.connectedIDs) {
//      if (connectionAttempts.containsKey(destID)) {// && (Integer) connectionAttempts.toString(destID) >= MAX_ATTEMPTS) {
//        if (StartNameServer.debugMode) GNRS.getLogger().severe("NIOEXCEPTION: Could not connect. Max attempts reached. = " + MAX_ATTEMPTS);
//        return false;
//      }
      if (connectedIDs.containsKey(destID) && connectedIDs.get(destID).isConnected()) { // connected
      //if (connectedIDs[destID] != null && connectedIDs[destID].isConnected()) { // connected
        send(destID, connectedIDs.get(destID), data);
//                connectionAttempts.put(destID, 0);

      } else if (connectedIDs.containsKey(destID) && connectedIDs.get(destID).isConnectionPending()) { // add to pending data
      //} else if (connectedIDs[destID] != null && connectedIDs[destID].isConnectionPending()) { // add to pending data
//                connectionAttempts.put(destID, 0);
        synchronized (this.pendingData) {
          List queue = (List) this.pendingData.get(connectedIDs.get(destID));
          if (queue == null) {
            queue = new ArrayList();
            this.pendingData.put(connectedIDs.get(destID), queue);
          }
          queue.add(ByteBuffer.wrap(data));
        }

      } else {
        numberOfConnectionsInitiated++;
//                if (StartNameServer.debugMode) GNS.getLogger().info("\tNIOSTAT\tconnection-event\t"
//                        + numberOfConnectionsInitiated + "\t" + destID + "\t");
        // new connection.
//				String host = ((String)ID_to_IP_Port.toString(id)).split(":")[0];
//				int port = Integer.parseInt(((String)ID_to_IP_Port.toString(id)).split(":")[1]);
        SocketChannel newSocketChannel = SocketChannel.open();
        newSocketChannel.configureBlocking(false);
        InetAddress address = nodeConfig.getNodeAddress(destID);
        int destPort = nodeConfig.getNodePort(destID);

        // Kick off connection establishment
        newSocketChannel.connect(new InetSocketAddress(address, destPort));

//        SocketChannel newSocketChannel = this.initiateConnection(address, destPort);
        SocketChannel socketChannel = connectedIDs.get(destID);
        //SocketChannel socketChannel = connectedIDs[destID];
        connectedIDs.put(destID, newSocketChannel);
        //connectedIDs[destID] = newSocketChannel;

        synchronized (this.pendingData) {
          // read old entries.
          List queue = null;
          if (this.pendingData.containsKey(socketChannel)) {
            queue = (List) this.pendingData.remove(socketChannel);
          }
          if (queue == null) {
            queue = new ArrayList();
          } else {
            if (queue.size() > 0) {

            }
          }

          queue.add(ByteBuffer.wrap(data));
          this.pendingData.put(newSocketChannel, queue);
          this.pendingChangeByNode.put(destID, false);
          //this.pendingChangeByNode[destID] = false;
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
  private void send(NodeIDType nodeId, SocketChannel socket, byte[] data) {

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
      this.pendingChangeByNode.put(nodeId, true);
      //this.pendingChangeByNode[nodeId] = true;
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
                  GNS.getLogger().severe("INVALID KEY: ");
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

    synchronized (this.connectedIDs) {
      // Finish the connection. If the connection operation failed
      // this will raise an IOException.
      try {
        socketChannel.finishConnect();
        socketChannel.socket().setKeepAlive(true);
      } catch (IOException e) {
        // Cancel the channel's registration with our selector
        GNS.getLogger().severe(e.getMessage());
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
      GNS.getLogger().severe("READ EXCEPTION, FORCED CLOSE CONNECTION.");
      key.cancel();
      socketChannel.close();
      return;
    }

    if (numRead == -1) {
      // Remote entity shut the socket down cleanly. Do the
      // same from our end and cancel the channel.
      GNS.getLogger().warning("REMOTE ENTITY SHUT DOWN SOCKET CLEANLY.");
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
        try {
          socketChannel.write(buf);
        } catch (IOException e) {
          // The remote forcibly closed the connection, cancel
          // the selection key and close the channel.
          GNS.getLogger().severe("WRITE EXCEPTION, FORCED CLOSE CONNECTION.");
          key.cancel();
          socketChannel.close();
          return;
        }

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
    for (int i = 0; i < 100; i++) {
      long delay = 100;
      Random r = new Random();

      delay = (long) ((1.0 + r.nextDouble() / 10.0) * delay);

      System.out.println(">>> " + delay);
    }
    System.exit(2);
    Object ID = args[0];
    // hack
    int integerVersionOfTheNodeId = Integer.parseInt(ID.toString());
    int port = 9000 + 10 * integerVersionOfTheNodeId;
    try {
      ByteStreamToJSONObjects worker = new ByteStreamToJSONObjects(null);
      new Thread(worker).start();
      NioServer server = new NioServer(ID, worker, null);
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

        int sendTo = (integerVersionOfTheNodeId + 1) % 2;

        if (sendTo != integerVersionOfTheNodeId && sendTo >= 0) // TODO : Fix this to test this method.
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

  @Override
  public void addPacketDemultiplexer(AbstractPacketDemultiplexer pd) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

@Override
public NodeConfig<NodeIDType> getNodeConfig() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public SSL_MODES getSSLMode() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public int sendToID(NodeIDType id, byte[] msg) throws IOException {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public int sendToAddress(InetSocketAddress isa, byte[] msg) throws IOException {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public boolean isDisconnected(NodeIDType node) {
	// TODO Auto-generated method stub
	return false;
}

}

class WakeupSelectorTask extends TimerTask {

  NioServer nioServer;

  public WakeupSelectorTask(NioServer nioServer) {
    this.nioServer = nioServer;
  }

  @Override
  public void run() {
    nioServer.wakeupSelector();
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
//		if (connectionStatus.containsKey(id)) status = (Integer) connectionStatus.toString(id);
//
//		// ID is connected
//		if (status == 2) {
//			// then connectedIDs must also contain a SocketChannel object.
//			if (connectedIDs.containsKey(id)) {
//				SocketChannel socketChannel = (SocketChannel) connectedIDs.toString(id);
//				send(socketChannel, data);
//			}
//		} else if (status == 1) { // connected initiated.
//			// toString socketChannel
//			SocketChannel socketChannel = (SocketChannel) connectedIDs.toString(id);
//			// Add to pendingData
//			synchronized (this.pendingData) {
//				List queue = (List) this.pendingData.toString(socketChannel);
//				if (queue == null) {
//					queue = new ArrayList();
//					this.pendingData.put(socketChannel, queue);
//				}
//				queue.add(ByteBuffer.wrap(data));
//			}
//		} else if (status == 0) { // establish connection and send.
//			System.out.println("SendToID:" + status);
//			String host = ((String)ID_to_IP_Port.toString(id)).split(":")[0];
//			int port = Integer.parseInt(((String)ID_to_IP_Port.toString(id)).split(":")[1]);
//
//			SocketChannel socketChannel = this.initiateConnection(host, port);
//			connectedIDs.put(id, socketChannel);
//			connectionStatus.put(id, 1); // change status to connection initiated.
//
//			// ABHIGYAN
//			synchronized (this.pendingData) {
//				List queue = (List) this.pendingData.toString(socketChannel);
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
//				int id = (Integer) IP_Port_to_ID.toString(hostIP + ":" + port);
//				System.out.println("CONNECTED to " + id);
//			}
//		}
//	}
//

class SendQueryWithDelay2<NodeIDType> extends TimerTask {

  JSONObject json;
  NodeIDType destID;
  NioServer<NodeIDType> nioServer;

  public SendQueryWithDelay2(NioServer<NodeIDType> nioServer, NodeIDType destID, JSONObject json) {
    this.json = json;
    this.destID = destID;
    this.nioServer = nioServer;
  }

  @Override
  public void run() {
    try {
      nioServer.sendToIDActual(destID, json);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
