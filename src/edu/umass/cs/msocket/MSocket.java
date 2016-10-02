/*******************************************************************************
 *
 * Mobility First - mSocket library
 * Copyright (C) 2013, 2014 - University of Massachusetts Amherst
 * Contact: arun@cs.umass.edu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Arun Venkataramani, Aditya Yadav, Emmanuel Cecchet.
 * Contributor(s): ______________________.
 *
 *******************************************************************************/

package edu.umass.cs.msocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.gns.Integration;
import edu.umass.cs.msocket.logger.MSocketLogger;
import edu.umass.cs.msocket.mobility.MobilityManagerClient;

/**
 * This class implements the MSocket, applications can use MSocket to connect to
 * a listening server
 * 
 * @author ayadav
 */

public class MSocket extends Socket implements MultipathInterface
{
	/**
	 * Denotes the type of connection msocket client wants to establish, 
	 * MSOCKET_SERVER establishes connection to msocket server.
	 * LEGACY_SERVER establishes connection to legacy socket server.
	 * 
	 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
	 * @version 1.0
	 */
	
  public static enum MSocketType {MSOCKET_SERVER, LEGACY_SERVER};
  /**
   * MSocket handshake timeout (used when connecting to a non MServerSocket to
   * not be blocked)
   */
  public static final int    MSOCKET_HANDSHAKE_TIMEOUT 		= 10000;

  /**
   * Keep alive frequency (default is 5 seconds)
   */
  public static final int            KEEP_ALIVE_FREQ           	= 5;

  // true if TcpNodelay set, false otherwise
  private boolean              setTcpNoDelay;
  private int                  setSoLinger;
  private boolean             keepAlive;
  private int                 trafficClass;
  private boolean             shutdownInput;
  private boolean             shutdownOutput;
  private boolean             isConnected;
  private boolean             isBound;
  private boolean             isClosed;
  
  protected ConnectionInfo    connectionInfo			= null;

  /**
   * The two IOStreams below are stored locally as an optimization so as to
   * prevent the creation of a new stream object each time get*Stream() is
   * called.
   */

  private InputStream         min                       = null;
  private OutputStream        mout                      = null;

  

  /**
   * socket channel used in supporting legacy socket bind connect operations
   */
  private SocketChannel     legacyChannel             	= null;

  /**
   * List of user provided addresses to bind (null if none has been provided)
   */
  private List<SocketAddress> bindpoints;

  /**
   * Maximum number of flowpath (0= no limit)
   */
  private int                 maxFlowPath               = 0;

  /**
   * Creates an unconnected MSocket.
   */
  public MSocket()
  {
	  try 
	  {
		legacyChannel = SocketChannel.open();
	  } catch (IOException e) 
	  {
		e.printStackTrace();
	  }
  }
  
  /**
   * Creates a socket and connects it to the specified remote address on the specified remote port.
   * The Socket will also bind() to the local address and port supplied. 
   * If the specified local address is null then the system will pick the address.
   * A local port number of zero will let the system pick up a free port in the bind operation.

   * @param address - the remote address
   * @param port - the remote port
   * @param localAddr - the local address the socket is bound to, or null for the anyLocal address.
   * @param the local port the socket is bound to or zero for a system selected free port.
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  public MSocket(InetAddress address, int port, InetAddress localAddr, int localPort) throws IOException
  {
	    this();
	    
	    if(localAddr == null)
		{
	    	Vector<InetAddress> Interfaces = CommonMethods.getActiveInterfaceInetAddresses();
	    	localAddr = Interfaces.get(1 % Interfaces.size());
		}
	    
	    bind(new InetSocketAddress(localAddr, localPort));
	    connect(new InetSocketAddress(address, port));
  }
  
  /**
   * Creates a MSocket and connects it to the specified remote host on the specified remote port. 
   * The MSocket will also bind() to the local address and port supplied.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * The it throws an UnknownHostException.
   * A local port number of zero will let the system pick up a free port in the bind operation.
   * 
   * @param host - the name of the remote host.
   * @param port - the remote port
   * @param localAddr - the local address the socket is bound to, or null for the anyLocal address.
   * @param localPort - the local port the socket is bound to, or zero for a system selected free port.
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  public MSocket(String host, int port, InetAddress localAddr, int localPort) throws IOException
  {
	  if(localAddr == null)
	  {
		  Vector<InetAddress> Interfaces = CommonMethods.getActiveInterfaceInetAddresses();
		  localAddr = Interfaces.get(1 % Interfaces.size());
	  }
	  
	  String serverAlias = host;
	  InetAddress serverIP = null;
	  int serverPort = port;
	  int typeOfCon = -1;
	  String stringGUID = "";
	
	  boolean connectSucc = false;
	  
		// handling connect time mobility
		do
		{
		  Random rand = new Random(System.currentTimeMillis());
		
		  long getSockAddrStart = System.currentTimeMillis();
		  List<InetSocketAddress> socketAddressFromGNS = Integration.getSocketAddressFromGNS(serverAlias);
		  long getSockAddrEnd = System.currentTimeMillis();
		
		  MSocketInstrumenter.updateSocketAddressFromGNS(getSockAddrEnd - getSockAddrStart);
		
		  InetSocketAddress serverSock = socketAddressFromGNS.get(rand.nextInt(socketAddressFromGNS.size()));
		  serverIP = serverSock.getAddress();
		  serverPort = serverSock.getPort();
		
		  long getGUIDStart = System.currentTimeMillis();
		  stringGUID = Integration.getGUIDOfAlias(serverAlias);
		  long getGUIDEnd = System.currentTimeMillis();
		
		  MSocketInstrumenter.updateGetGUID(getGUIDEnd - getGUIDStart);
		
		  typeOfCon = MSocketConstants.CON_TO_GNSNAME;
		
		  MSocketInstrumenter.updateNumConnAttempt();
		
		  try
		  {
		    connect(serverAlias, serverIP, serverPort, localAddr, localPort, typeOfCon, stringGUID);
		    connectSucc = true;
		  }
		  catch (Exception ex)
		  {
			  MSocketLogger.getLogger().fine(ex.getMessage());
		    //ex.printStackTrace();
		    connectSucc = false;
		  }
		}
		while (!connectSucc);
		
		connectionInfo.setServerOrClient(MSocketConstants.CLIENT);
		
		registerWithClientManager();
		
		//localTimer = new Timer();
		//startLocalTimer();
		
		//TimerTaskClass Obj = new TimerTaskClass(this);
		//(new Thread(Obj)).start();
		
		KeepAliveStaticThread.registerForKeepAlive(connectionInfo);
		
		isConnected = true;
		isBound = true;
  }

  /**
   * 
   * Creates a MSocket and connects it to the specified port number on the name.
   * It creates as many flowpaths are there are active interfaces at the device at the time of 
   * calling.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * The it throws an UnknownHostException.
   * 
   * @param host - the host name, or null for the loopback address.
   * @param port - the port number.
   * @throws UnknownHostException - if the host is null or can't be resolved in GNS
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  public MSocket(String host, int port) throws UnknownHostException, IOException
  {
	this(host, port, null, 0);
	
	Vector<InetAddress> interfaces = CommonMethods.getActiveInterfaceInetAddresses();
	int numFlowpaths = interfaces.size();
	
	for(int i=2; i<=numFlowpaths; i++)
	{
		InetSocketAddress localBindAdd = new InetSocketAddress(interfaces.get(i % interfaces.size()), 0);
		FlowPath fp = addFlowPath(localBindAdd);
		
		if(fp == null)
		{
			MSocketLogger.getLogger().fine("addFlowPath failed "+localBindAdd);
		} else
		{
			MSocketLogger.getLogger().fine("addFlowPath succeded"+localBindAdd);
		}
	}
  }

  /**
   * Creates a MSocket and connects it to the specified port number at the specified IP address. 
   * It creates as many flowpaths are there are active interfaces at the device at the time of 
   * calling.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * In other words, it is equivalent to specifying an address of the loopback interface.
   * 
   * @param address - the IP address.
   * @param port - the port number.
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  public MSocket(InetAddress address, int port) throws IOException
  {
	// open as many as there are interfaces
    this(address, port, null, 0);
    
    Vector<InetAddress> interfaces = CommonMethods.getActiveInterfaceInetAddresses();
	int numFlowpaths = interfaces.size();
	
	for(int i=2; i<=numFlowpaths; i++)
	{
		InetSocketAddress localBindAdd = new InetSocketAddress(interfaces.get(i % interfaces.size()), 0);
		FlowPath fp = addFlowPath(localBindAdd);
		
		if(fp == null)
		{
			MSocketLogger.getLogger().fine("addFlowPath failed "+localBindAdd);
		} else
		{
			MSocketLogger.getLogger().fine("addFlowPath succeded"+localBindAdd);
		}
	}
  }
  
  /**
   * Creates MSocket and connects it to the specified port number on the named host.
   * stream flag is ignored and TCP sockets are always created.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * The it throws an UnknownHostException.
   * 
   * @param host - the host name
   * @param port - the port number.
   * @param stream - flag always ignored.
   * @throws IOException - if an I/O error occurs when creating the socket.
   * @throws UnknownHostException - if host is null or can't be resolved in GNS
   */
  @Deprecated
  public MSocket(String host, int port, boolean stream) throws IOException
  {
	  this(host, port);
  }
  
  /**
   * Creates a MSocket and connects it to the specified port number at the specified IP address.
   * stream argument is ignored and a TCP socket is always created.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * In other words, it is equivalent to specifying an address of the loopback interface.
   * 
   * @param host - the IP address.
   * @param port - the port number.
   * @param stream - flag always ignored.
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  @Deprecated
  public MSocket(InetAddress host, int port, boolean stream) throws IOException
  {
	  this(host, port);
  }
  
  /**
   * Constructor not supported by MSocket, will throw an UnsupportedOperationException
   * 
   * @param proxy
   * @throws UnsupportedOperationException
   */
  @Deprecated
  public MSocket(Proxy proxy)
  {
	  throw new UnsupportedOperationException("Constructor not supported by MSocket");
  }
  
  /**
   * Creates a MSocket and connects it to the specified port number at the specified IP address. 
   * It creates as many flowpaths are there are active interfaces at the device at the time of 
   * calling.
   * If the specified host is null it is the equivalent of specifying the address as InetAddress.getByName(null). 
   * In other words, it is equivalent to specifying an address of the loopback interface.
   * 
   * @param address - the IP address.
   * @param port - the port number.
   * @throws IOException - if an I/O error occurs when creating the socket.
   */
  public MSocket(InetAddress address, int port, MSocketType serverType) throws IOException
  {
	/*if(serverType == MSocketType.LEGACY_SERVER)
	{
	// open as many as there are interfaces
    this(address, port, null, 0);
    
    Vector<InetAddress> interfaces = CommonMethods.getActiveInterfaceInetAddresses();
	int numFlowpaths = interfaces.size();
	
	for(int i=2; i<=numFlowpaths; i++)
	{
		InetSocketAddress localBindAdd = new InetSocketAddress(interfaces.get(i % interfaces.size()), 0);
		FlowPath fp = addFlowPath(localBindAdd);
		
		if(fp == null)
		{
			MSocketLogger.getLogger().fine("addFlowPath failed "+localBindAdd);
		} else
		{
			MSocketLogger.getLogger().fine("addFlowPath succeded"+localBindAdd);
		}
	}
	}*/
  }

  /**
   * Binds the MSocket locally to the specified address
   * 
   * @see java.net.Socket#bind(java.net.SocketAddress)
   */
  public void bind(SocketAddress bindpoint) throws IOException
  {
    ArrayList<SocketAddress> binding = new ArrayList<SocketAddress>();
    binding.add(bindpoint);
    bind(binding);
    // legacyChannel = SocketChannel.open();
    // legacyChannel.socket().bind(bindpoint);
    legacyChannel.socket().bind(bindpoint);
  }

  /**
   * Closes the MSocket and releases the state of the socket. MSocket close()
   * doesn't close MobilityManagerClient
   * 
   * @see java.net.Socket#close()
   */
  public void close() throws IOException
  {
    if ((connectionInfo.getMSocketState() == MSocketConstants.ACTIVE)
        || (connectionInfo.getMSocketState() == MSocketConstants.CLOSE_WAIT))
    {
      if (connectionInfo.getMSocketState() == MSocketConstants.ACTIVE)
      {
    	  connectionInfo.setMSocketState(MSocketConstants.FIN_WAIT_1);
      }
      else if (connectionInfo.getMSocketState() == MSocketConstants.CLOSE_WAIT)
      {
    	  connectionInfo.setMSocketState(MSocketConstants.LAST_ACK);
      }

      MSocketLogger.getLogger().fine("close() called");
      sendCloseControlmesg();
      MSocketLogger.getLogger().fine("sendCloseControlmesg() done MSocket state " 
    		  		+ connectionInfo.getMSocketState());

      while (connectionInfo.getMSocketState() != MSocketConstants.CLOSED)
      {
        try
        {
          // MSocketLogger.getLogger().fine("MSocket type " + cinfo.getServerOrClient() +
          // "cinfo.getMSocketState() " + cinfo.getMSocketState());
          // for synchronization, so that migration setup control read and this
          // input stream read does not go hand in hand
          // add sync here, so that only one thread blocks on
          // selector, so that thread is unblocked on data arrival
          synchronized (connectionInfo.getInputStreamSelectorMonitor())
          {
        	  connectionInfo.blockOnInputStreamSelector();
          }

          connectionInfo.setState(ConnectionInfo.READ_WRITE, true);
          connectionInfo.multiSocketRead();
          connectionInfo.setState(ConnectionInfo.ALL_READY, true);
        }
        catch (IOException ex)
        {
        	connectionInfo.setState(ConnectionInfo.ALL_READY, true);
          if (connectionInfo.getMSocketState() != MSocketConstants.CLOSED)
          {
            MSocketLogger.getLogger().fine("Exception during recv ACV, "
            		+ "trying again, state changed to "
                + "ALL_READY for migration to happen. cinfo.getMSocketState() " 
            		+ connectionInfo.getMSocketState());
            ex.printStackTrace();
          }

        }
      }
    }
    isClosed = true;
  }
  
  /**
   * Connects this socket to the server.
   * @see java.net.Socket#connect(java.net.SocketAddress)
   */
  public void connect(SocketAddress endpoint) throws IOException
  {
	  connect(endpoint, 0);
  }
  
  /**
   * Connects this socket to the server with a specified timeout value.
   * timeout is ignored and it blocks until the connection completes
   * The connection will then block until established or an error occurs.
   */
  public void connect(SocketAddress endpoint, int timeout) throws IOException
  {
	  	//FIXME: need to do something with the timeout
		long connectStart = System.currentTimeMillis();
		legacyChannel.connect(endpoint);
		while (!legacyChannel.finishConnect());
	  
	    long connectEnd = System.currentTimeMillis();
	    long connectTime = connectEnd - connectStart;
	
	    Socket socket = legacyChannel.socket();
	    InetAddress serverIP = ((InetSocketAddress) endpoint).getAddress();
	    int serverPort = ((InetSocketAddress) endpoint).getPort();
	    int typeOfCon = MSocketConstants.CON_TO_IP;
	    
	
	    MSocketLogger.getLogger().fine("Connected to server at " + socket.getInetAddress() 
	    	+ ":" + socket.getPort() + " - local port "
	        + socket.getLocalPort() + " - local IP " 
	    	+ socket.getLocalAddress());
	    setupControlClient("", serverIP, serverPort, typeOfCon, "", 
	    		legacyChannel, socket, connectTime);
	
	    connectionInfo.setMSocketState(MSocketConstants.ACTIVE);
	
	    connectionInfo.setState(ConnectionInfo.ALL_READY, true);
	
	    connectionInfo.setServerOrClient(MSocketConstants.CLIENT);
	
	    registerWithClientManager();
	
	    //localTimer = new Timer();
	    //startLocalTimer();
	    //TimerTaskClass Obj = new TimerTaskClass(this);
	    //(new Thread(Obj)).start();
	    
	    KeepAliveStaticThread.registerForKeepAlive(connectionInfo);   
	    isConnected = true;
  }
  
  /**
   * @see java.net.Socket#getInetAddress()
   */
  public InetAddress getInetAddress()
  {
    if (connectionInfo == null)
      return null;
    else
    {
      return connectionInfo.getServerIP();
    }
  }

  /**
   * Local address of any flowpath is returned
   * 
   * @see java.net.Socket#getLocalAddress()
   */
  public InetAddress getLocalAddress()
  {
    if (connectionInfo == null)
      return null;
    else
    {
      return connectionInfo.getActiveSocket
    		  (MultipathPolicy.MULTIPATH_POLICY_RANDOM).getSocket().getLocalAddress();
    }
  }

  /**
   * Remote port of any flowpath is returned
   * @see java.net.Socket#getPort()
   */
  public int getPort()
  {
    if (connectionInfo == null)
      return -1;
    else
    {
      return connectionInfo.getActiveSocket
    		  (MultipathPolicy.MULTIPATH_POLICY_RANDOM).getSocket().getPort();
    }
  }

  /**
   * Local port of any flowpath is returned
   * 
   * @see java.net.Socket#getLocalPort()
   */
  public int getLocalPort()
  {
    if (connectionInfo == null)
      return -1;
    else
    {
      return connectionInfo.getActiveSocket
    		  (MultipathPolicy.MULTIPATH_POLICY_RANDOM).getSocket().getLocalPort();
    }
  }

  /**
   * Remote socket address of any flowpath is returned
   * 
   * @see java.net.Socket#getRemoteSocketAddress()
   */
  public SocketAddress getRemoteSocketAddress()
  {
    if (connectionInfo == null)
      return null;
    else
    {
      return connectionInfo.getActiveSocket
    		  (MultipathPolicy.MULTIPATH_POLICY_RANDOM).getSocket().getRemoteSocketAddress();
    }
  }

  /**
   * Local socket address of any flowpath is returned
   * 
   * @see java.net.Socket#getLocalSocketAddress()
   */
  public SocketAddress getLocalSocketAddress()
  {
    if (connectionInfo == null)
      return null;
    else
    {
      return connectionInfo.getActiveSocket
    		  (MultipathPolicy.MULTIPATH_POLICY_RANDOM).getSocket().getLocalSocketAddress();
    }
  }

  /**
   * Returns the address of the server, this MSocket is connected to
   * 
   * @return
   */
  public InetSocketAddress getServerAddress()
  {
    return new InetSocketAddress(connectionInfo.getServerIP(), 
    		connectionInfo.getServerPort());
  }

  /**
   * MSocket doesn't exposes underlying socket channels.
   * 
   * @see java.net.Socket#getChannel()
   * @throws UnsupportedOperationException
   */
  @Deprecated
  public SocketChannel getChannel()
  {
	  throw new UnsupportedOperationException("MSocket doesn't exposes underlying socket channels");
  }

  /**
   * Returns the Input stream, should be used for all MSocket read operations
   * 
   * @see java.net.Socket#getInputStream()
   */
  public InputStream getInputStream() throws IOException
  {
    if (connectionInfo == null)
      throw new SocketException("Socket is not connected");

    if (this.min != null)
      return this.min;
    min = new MWrappedInputStream(connectionInfo);
    return this.min;
  }

  /**
   * Returns the output stream, should be used for all mSocket write operations
   * 
   * @see java.net.Socket#getOutputStream()
   */
  public OutputStream getOutputStream() throws IOException
  {
    if (connectionInfo == null)
      throw new SocketException("Socket is not connected");

    if (this.mout != null)
      return this.mout;
    mout = new MWrappedOutputStream(connectionInfo);
    return this.mout;
  }

  /**
   * Sets TcpNodelay on all the active flow paths, between the server and
   * client
   * 
   * @see java.net.Socket#setTcpNoDelay(boolean)
   */
  public void setTcpNoDelay(boolean on) throws SocketException
  {
    setTcpNoDelay = on;
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setTcpNoDelay(on);
    }
  }

  /**
   * @see java.net.Socket#getTcpNoDelay()
   */
  public boolean getTcpNoDelay() throws SocketException
  {
    return setTcpNoDelay;
  }

  /**
   * sets SoLinger on all the active flow paths, between the server and the
   * client
   * 
   * @see java.net.Socket#setSoLinger(boolean, int)
   */
  public void setSoLinger(boolean on, int linger) throws SocketException
  {
    setSoLinger = linger;
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setSoLinger(on, linger);
    }
  }

  /**
   * @see java.net.Socket#getSoLinger()
   */
  public int getSoLinger() throws SocketException
  {
    return setSoLinger;
  }

  /**
   * mSockets do not support urgent data. This method systematically throws an
   * IOException.
   * 
   * @see java.net.Socket#sendUrgentData(int)
   */
  public void sendUrgentData(int data) throws IOException
  {
    throw new IOException("Urgent data is not supported with mSockets");
  }

  /**
   * mSockets do not support urgent data. This method systematically throws a
   * SocketException.
   * 
   * @see java.net.Socket#setOOBInline(boolean)
   */
  public void setOOBInline(boolean on) throws SocketException
  {
    throw new SocketException("Urgent data is not supported with mSockets");
  }

  /**
   * mSockets do not support urgent data. This method systematically returns
   * false.
   * 
   * @see java.net.Socket#getOOBInline()
   */
  public boolean getOOBInline() throws SocketException
  {
    return false;
  }

  /**
   * mSockets currently do not support timeouts. This method systematically
   * throws a SocketException.
   * 
   * @see java.net.Socket#setSoTimeout(int)
   */
  public void setSoTimeout(int timeout) throws SocketException
  {
    throw new SocketException("Timeouts are currently not supported with mSockets");
  }

  /**
   * mSockets currently do not support timeouts. This method systematically
   * returns 0.
   * 
   * @see java.net.Socket#getSoTimeout()
   */
  public int getSoTimeout() throws SocketException
  {
    return 0;
  }

  /**
   * Sets the send buffer size on all active flow paths, between the server the
   * client
   */
  public void setSendBufferSize(int size) throws SocketException
  {
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setSendBufferSize(size);
    }
  }

  /**
   * returns the sum of sendbuffer sizes on all flowpaths
   */
  public int getSendBufferSize() throws SocketException
  {
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    return connectionInfo.getSendBufferSize();
  }

  /**
   * Sets the recv buffer size on all the active flowpaths, between the server
   * and the client
   * 
   * @see java.net.Socket#setReceiveBufferSize(int)
   */
  public void setReceiveBufferSize(int size) throws SocketException
  {
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setReceiveBufferSize(size);
    }
  }

  /**
   * @see java.net.Socket#getReceiveBufferSize()
   */
  public int getReceiveBufferSize() throws SocketException
  {
	  if (connectionInfo == null)
		{
			throw new SocketException("socket not connected");
		}
		return connectionInfo.getReceiveBufferSize();
  }

  /**
   * @see java.net.Socket#getTrafficClass()
   */
  public int getTrafficClass() throws SocketException
  {
    return trafficClass;
  }

  /**
   * Sets the traffic class on all the active flow paths, between the server and
   * the client
   * 
   * @see java.net.Socket#setTrafficClass(int)
   */
  public void setTrafficClass(int tc) throws SocketException
  {
    trafficClass = tc;
    if (connectionInfo == null)
    {
    	throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setTrafficClass(tc);
    }
  }

  /**
   * Sets keep alive on all the active flow paths, between the server and the
   * client.
   * 
   * @see java.net.Socket#setKeepAlive(boolean)
   */
  public void setKeepAlive(boolean on) throws SocketException
  {
    keepAlive = on;
    if (connectionInfo == null)
    {
      throw new SocketException("socket not connected");
    }
    else
    {
    	connectionInfo.setKeepAlive(on);
    }
  }

  /**
   * @see java.net.Socket#getKeepAlive()
   */
  public boolean getKeepAlive() throws SocketException
  {
    return keepAlive;
  }

  /**
   * mSockets currently do not support timeouts. This method systematically
   * throws a SocketException.
   * 
   * @see java.net.Socket#setReuseAddress(boolean)
   */
  public void setReuseAddress(boolean on) throws SocketException
  {
    throw new SocketException("setReuseAddress is not supported by mSocket");
  }

  /**
   * mSockets currently do not support reuse address. This method systematically
   * returns false.
   * 
   * @see java.net.Socket#getReuseAddress()
   */
  public boolean getReuseAddress() throws SocketException
  {
    return false;
  }

  /**
   * @see java.net.Socket#shutdownInput()
   */
  public void shutdownInput() throws IOException
  {
    shutdownInput = true;
  }

  /**
   * @see java.net.Socket#shutdownOutput()
   */
  public void shutdownOutput() throws IOException
  {
    shutdownOutput = true;
  }

  /**
   * @see java.net.Socket#toString()
   */
  public String toString()
  {
    return this.toString();
  }

  /**
   * @see java.net.Socket#isConnected()
   */
  public boolean isConnected()
  {
    return isConnected;
  }

  /**
   * @see java.net.Socket#isBound()
   */
  public boolean isBound()
  {
    return isBound;
  }

  /**
   * @see java.net.Socket#isClosed()
   */
  public boolean isClosed()
  {
    return isClosed;
  }

  /**
   * @see java.net.Socket#isInputShutdown()
   */
  public boolean isInputShutdown()
  {
    return shutdownInput;
  }

  /**
   * @see java.net.Socket#isOutputShutdown()
   */
  public boolean isOutputShutdown()
  {
    return shutdownOutput;
  }

  /**
   * Sets the performance preferences over all the active flow paths
   * 
   * @see java.net.Socket#setPerformancePreferences(int, int, int)
   */
  public void setPerformancePreferences(int connectionTime, int latency, int bandwidth)
  {
    if (connectionInfo == null)
    {
    }
    else
    {
    	connectionInfo.setPerformancePreferences(connectionTime, latency, bandwidth);
    }
  }

  /**
   * Gets the multiPath data Write Policy.
   * 
   * @return
   */
  public MultipathPolicy getMultipathPolicy()
  {
    return connectionInfo.getMultipathPolicy();
  }

  /**
   * Sets the multiPath data Write Policy. Choose among
   * {@link MSocket#MULTIPATH_POLICY_RANDOM} to randomly send over paths,
   * {@link MSocket#MULTIPATH_POLICY_DEFAULT} for an optimized multipath policy,
   * {@link MSocket#MULTIPATH_POLICY_UNIFORM} to sends uniformly over all the
   * paths or {@link MSocket#MULTIPATH_POLICY_OUTSTAND_RATIO} to choose the
   * least outstanding bytes path, to send the data
   * 
   * @param multipathPolicy the multipath policy to use
   */
  public void setMultipathPolicy(MultipathPolicy multipathPolicy)
  {
	  connectionInfo.setMultipathPolicy(multipathPolicy);
  }
  
  /*
   * Implementation related to MultipathInterface
   */
  @Override
  public void bind(List<SocketAddress> bindings) throws IOException
  {
    this.bindpoints = bindings;
  }

  @Override
  public List<SocketAddress> getBindings()
  {
    return bindpoints;
  }

  /**
   * @see edu.umass.cs.msocket.MultipathInterface#addFlowPath(java.net.SocketAddress)
   */
  @Override
  public FlowPath addFlowPath(SocketAddress binding)
  {
    if ( (maxFlowPath != 0) && (connectionInfo.getAllSocketInfo().size() >= maxFlowPath) )
      return null;

    FlowPathResult res = connectionInfo
        .addSocketToFlow(connectionInfo.getConnID(), 
        	SetupControlMessage.ADD_SOCKET, -1, binding == null
            ? null
            : ((InetSocketAddress) binding).getAddress(),
            binding == null ? 0 : ((InetSocketAddress) binding).getPort(), -1);
    if (res.getSuccessful())
    {
      SocketInfo sockInfo = connectionInfo.getSocketInfo(res.getSocketID());
      FlowPath nfp = new FlowPath(res.getSocketID(), 
    		  sockInfo.getSocket().getLocalSocketAddress(), sockInfo.getSocket()
          .getRemoteSocketAddress());
      return nfp;
    }
    else
    {
      return null;
    }
  }

  /**
   * @see edu.umass.cs.msocket.MultipathInterface#getActiveFlowPaths()
   */
  @Override
  public List<FlowPath> getActiveFlowPaths()
  {
    List<FlowPath> activeFlowPaths = new LinkedList<FlowPath>();
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(connectionInfo.getAllSocketInfo());

    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // check active
      if (Obj.getStatus())
      {
        FlowPath fp = new FlowPath(Obj.getSocketIdentifer(), Obj.getSocket().getLocalSocketAddress(), Obj.getSocket()
            .getRemoteSocketAddress());

        activeFlowPaths.add(fp);
      }
    }
    return activeFlowPaths;
  }

  @Override
  public void removeFlowPath(FlowPath flowpath) throws IOException
  {
    int socketIdentifier = flowpath.getFlowPathId();
    connectionInfo.removeSocketFromFlow(socketIdentifier);
  }

  @Override
  public void setMaxFlowPath(int limit)
  {
    this.maxFlowPath = limit;
  }

  @Override
  public void setFlowPathPolicy(MultipathPolicy policy)
  {
	  connectionInfo.setMultipathPolicy(policy);
  }

  @Override
  public void migrateFlowPath(FlowPath flowpath, InetAddress localAddress, int localPort)
  {
    int socketIdentifier = flowpath.getFlowPathId();
    connectionInfo.migrateSocketwithId(localAddress, localPort, 
    		socketIdentifier, MSocketConstants.CLIENT_MIG);
  }
  
  /**
   * Returns the connection ID of the MSocket connection,
   * This should be same for the server and the client side MSocket.
   * @return
   */
  public long getConnID()
  {
	  return this.connectionInfo.getConnID();
  }
  
	 
  // protected methods
   //
   // private methods
   //
	  
  /**
   * register with mobility manager
   * 
   * @throws SocketException
   * @throws UnknownHostException
   */
  private void registerWithClientManager() throws SocketException, UnknownHostException
  {
    MobilityManagerClient.registerWithManager(connectionInfo);
  }

  /**
   * Client calls this
   * 
   * @param name
   * @param port
   * @throws Exception
   */
  private void connect(String serverAlias, InetAddress serverIP, 
		  int serverPort, InetAddress localIP, int localPort, 
		  int typeOfCon, String stringGUID) throws Exception
  {
    legacyChannel = SocketChannel.open();
    
    //bind(new InetSocketAddress(Interfaces.get(1 % Interfaces.size()), 0));
    bind(new InetSocketAddress(localIP, localPort));

    MSocketLogger.getLogger().fine("connecting to server " 
    				+ new InetSocketAddress(serverIP, serverPort));

    long connectStart = System.currentTimeMillis();
    legacyChannel.connect(new InetSocketAddress(serverIP, serverPort));
    while (!legacyChannel.finishConnect())
      ;
    long connectEnd = System.currentTimeMillis();
    long connectTime = connectEnd - connectStart;

    Socket socket = legacyChannel.socket();

    MSocketLogger.getLogger().fine("Connected to server at " 
    			+ socket.getInetAddress() + ":" + socket.getPort() + "local port "
        + socket.getLocalPort() + "local IP " + socket.getLocalAddress());
    setupControlClient(serverAlias, serverIP, serverPort, typeOfCon, stringGUID, legacyChannel, socket,
        connectTime);

    connectionInfo.setMSocketState(MSocketConstants.ACTIVE);

    connectionInfo.setState(ConnectionInfo.ALL_READY, true);
  }

  private void sendCloseControlmesg() throws IOException
  {
    ((MWrappedOutputStream) (getOutputStream())).write(null, 0, 0, DataMessage.FIN);
  }

  /**
   * Client writes first, then reads Exchanged int he beginning of connection
   * for handshaking
   * 
   * @throws Exception
   */
  private void setupControlClient(String serverAlias, InetAddress serverIP, int serverPort, 
		  int typeOfCon, String stringGUID, 
		  SocketChannel dataChannel, Socket socket, long connectTime)
      throws IOException
  {
    int nextSocketIdentifier = 1;
    long localConnID = (long) (Math.random() * Long.MAX_VALUE);
    boolean isNewConnection = false;

    if (connectionInfo == null)
    {
      isNewConnection = true;
    }

    Vector<InetAddress> localInterface = CommonMethods.getActiveInterfaceInetAddresses();
    InetAddress controllerIP = null;

    if (localInterface.size() > 0)
    {
      controllerIP = localInterface.get(0);
    }

    UDPControllerHashMap.startUDPController(controllerIP);

    int UDPControllerPort = -1;
    UDPControllerPort = UDPControllerHashMap.getUDPContollerPort(controllerIP);

    if (UDPControllerPort == -1)
    {
      MSocketLogger.getLogger().fine("New connection UDPControllerPort " + UDPControllerPort);
    }

    SetupControlMessage scm = null;
    byte[] GUID = new byte[SetupControlMessage.SIZE_OF_GUID];

    if (stringGUID.length() > 0)
    {
      MSocketLogger.getLogger().fine("serverGuid: " + stringGUID);
      GUID = CommonMethods.hexStringToByteArray(stringGUID);
      MSocketLogger.getLogger().fine("GUID to be sent: " + GUID + " length " + GUID.length);
    }

    SetupControlMessage.setupControlWrite(controllerIP, localConnID, 
    		SetupControlMessage.NEW_CON_MESG, UDPControllerPort,
        dataChannel, nextSocketIdentifier, -1, GUID, connectTime, null);
    
    // Read remote port, address, and flowID
    scm = SetupControlMessage.setupControlRead(dataChannel);

    if (isNewConnection)
    {
      // flowID is computed as average of both proposals for new connections
      long connID = (localConnID + scm.connID) / 2;
      MSocketLogger.getLogger().fine("Created new flow ID " + connID);

      connectionInfo = new ConnectionInfo(connID, null);

      setupConnectionInfoFields(serverAlias, serverIP, serverPort, typeOfCon, stringGUID);
      
      connectionInfo.setupClientController(scm);

      UDPControllerHashMap.registerWithController(controllerIP, connectionInfo);

      connectionInfo.setControllerIP(controllerIP);

      // some times due to race condition, channel is treated
      // as blocking, need to set it to non blocking here
      dataChannel.configureBlocking(false);
      // storing primary socket
      SocketInfo sockInfo = new SocketInfo(dataChannel, socket, nextSocketIdentifier);

      // initial estimation, 1 pkt per RTT.

      sockInfo.setEstimatedRTT(connectTime);

      connectionInfo.addSocketInfo(nextSocketIdentifier, sockInfo);
      nextSocketIdentifier++;

      connectionInfo.inputQueuePutSocketInfo(sockInfo);
      connectionInfo.outputQueuePutSocketInfo(sockInfo);

      // open multipath sockets
      // 0th socket already connected
      /*int i = 1;
      for (i = 1; i < numberOfSockets; i++)
      {
        cinfo.addSocketToFlow(flowID, SetupControlMessage.ADD_SOCKET, -1, null, -1, -1);
      }*/
    }
  }

  // Write local port, address, and flowID
  private void setupConnectionInfoFields(String serverAlias, InetAddress serverIP, 
		  int serverPort, int typeOfCon,
		  String stringGUID)
  {
	  connectionInfo.setServerAlias(serverAlias);
	  connectionInfo.setServerIP(serverIP);
	  connectionInfo.setServerPort(serverPort);
	  connectionInfo.setTypeOfCon(typeOfCon);
	  connectionInfo.setServerGUID(stringGUID);
  }
  
  // private constructors
  /**
   * Creates a new <code>MSocket</code> object connecting it to the given server
   * name registered in the GNS
   * 
   * @param serverName name of the MServerSocket registered in the GNS
   * @throws Exception
   * 
   */
  @Deprecated
  private MSocket(String serverGUID) throws Exception
  {
    InetAddress serverIP = null;
    int serverPort = -1;
    int typeOfCon = -1;
    String stringGUID = "";

    Random rand = new Random();
    List<InetSocketAddress> socketAddressFromGNS = 
    		Integration.getSocketAddressFromGNS(serverGUID);
    typeOfCon = MSocketConstants.CON_TO_GNSGUID;

    InetSocketAddress serverSock = socketAddressFromGNS.get
    		(rand.nextInt(socketAddressFromGNS.size()));
    serverIP = serverSock.getAddress();
    serverPort = serverSock.getPort();
    stringGUID = Integration.getGUIDOfAlias(serverGUID);

    
    Vector<InetAddress> Interfaces = CommonMethods.getActiveInterfaceInetAddresses();
    InetAddress localIP = Interfaces.get(1 % Interfaces.size());
    int localPort = 0;
    
    connect(serverGUID, serverIP, serverPort, localIP, localPort, typeOfCon, stringGUID);
    connectionInfo.setServerOrClient(MSocketConstants.CLIENT);

    registerWithClientManager();

    //localTimer = new Timer();
    //startLocalTimer();

    //TimerTaskClass Obj = new TimerTaskClass(this);
    //(new Thread(Obj)).start();
    KeepAliveStaticThread.registerForKeepAlive(connectionInfo);

    isConnected = true;
    isBound = true;
  }
  
  /**
   * Interface for experimental use
   * 
   * @param endpoint
   * @param numSockets
   * @param interfaceNum
   * @throws IOException
   */
  private MSocket(SocketAddress endpoint, int numSockets, int interfaceNum) throws IOException
  {
    Vector<InetAddress> Interfaces = CommonMethods.getActiveInterfaceInetAddresses();
    legacyChannel = SocketChannel.open();

    // socket are distributed to interfaces
    // by socketID%NumInterfaces function, first socket has ID 1
    if (Interfaces.size() > 0)
    {
      if (numSockets == 1)
      {
        legacyChannel.socket().bind(new InetSocketAddress(Interfaces.get(interfaceNum), 0));
      }
      else
      {
        legacyChannel.socket().bind(new InetSocketAddress(Interfaces.get
        		(1 % Interfaces.size()), 0));
      }
    }

    long connectStart = System.currentTimeMillis();
    legacyChannel.connect(endpoint);
    while (!legacyChannel.finishConnect())
      ;
    long connectEnd = System.currentTimeMillis();
    long connectTime = connectEnd - connectStart;

    Socket socket = legacyChannel.socket();
    InetAddress serverIP = ((InetSocketAddress) endpoint).getAddress();
    int serverPort = ((InetSocketAddress) endpoint).getPort();
    int typeOfCon = MSocketConstants.CON_TO_IP;
    int numberOfSockets = numSockets;

    MSocketLogger.getLogger().fine("Connected to server at " + socket.getInetAddress() 
    	+ ":" + socket.getPort() + " - local port "
        + socket.getLocalPort() + " - local IP " + socket.getLocalAddress());
    setupControlClient("", serverIP, serverPort, typeOfCon, "", legacyChannel, 
    		socket, connectTime);

    connectionInfo.setMSocketState(MSocketConstants.ACTIVE);

    connectionInfo.setState(ConnectionInfo.ALL_READY, true);

    connectionInfo.setServerOrClient(MSocketConstants.CLIENT);

    registerWithClientManager();

    //localTimer = new Timer();
    //startLocalTimer();

    //TimerTaskClass Obj = new TimerTaskClass(this);
    //(new Thread(Obj)).start();
    
    KeepAliveStaticThread.registerForKeepAlive(connectionInfo);

    isConnected = true;
  }
}


//commented code
/**
 * 
 * TODO: startLocalTimer definition.
 *
 */
/*protected void startLocalTimer()
{
  localTimer.scheduleAtFixedRate(new TimerTask()
  {
    @Override
    public void run()
    {
      if (!cinfo.getTimerStatus()) // stop the timer
      {
        localTimer.cancel();
        return;
      }

      cinfo.updateLocalClock();
    }
  }, TIMER_TICK, TIMER_TICK);
}*/
