/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.proxy.legacyforwarder;

public class LegacyProxyMSocket 
{
	/*public static final int GET            			= 1;
	public static final int SET            			= 2;

	  private SocketChannel   TCPChannel     			= null;
	  private static Logger   log            			= Logger.getLogger(ProxyMSocket.class.getName());
	  private int             socketType     			= -1;                                       // denotes
	                                                                                           		// the
	                                                                                           		// return
	                                                                                           		// type
	                                                                                           		// of
	                                                                                           		// accepted
	                                                                                           		// socket
	  
	  private byte[]          GUID           			= null;                                     // GUID
	                                                                                           		// of
	                                                                                           		// the
	                                                                                           		// control
	                                                                                           		// socket
	  
	  private LegacyProxyForwarder  		pForwarder     			= null;
	  
	  private int             				serverOrClient 			= -1;
	  private int             				proxyId        			= -1;
	  private boolean         				working        			= true;

	  private boolean         				lock           			= false;                                    // whether
	                                                                                           		// some
	                                                                                           		// thread
	                                                                                           		// has
	                                                                                           		// acquired
	                                                                                           		// lock
	                                                                                           		// or
	                                                                                           		// not

	  public LegacyProxyMSocket(SocketChannel sc, LegacyProxyForwarder pForwarder) throws IOException
	  {
	    GUID = new byte[SetupControlMessage.SIZE_OF_GUID];
	    TCPChannel = sc;
	    this.pForwarder = pForwarder;
	    setupControlServer(TCPChannel);
	  }

	  public synchronized void setProxyInfo(int serverOrClient, int proxyId)
	  {
	    this.proxyId = proxyId;
	    this.serverOrClient = serverOrClient;
	  }

	  public int getProxyId()
	  {
	    return proxyId;
	  }

	  public int getServerOrClient()
	  {
	    return serverOrClient;
	  }

	  public SocketChannel getUnderlyingChannel()
	  {
	    return TCPChannel;
	  }

	  // Write local port, address, and flowID
	  public void setupControlWrite(InetAddress ClientIP, int ClientUDPPort, long FlowID, int AckSeqNum, int MesgType,
			  int SocketId, int ProxyId, byte[] GUID, SocketChannel SCToUse) throws IOException
	  {

	    SetupControlMessage scm = new SetupControlMessage(ClientIP, ClientUDPPort, FlowID, AckSeqNum, MesgType,
	        SocketId, ProxyId, GUID);
	    ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());

	    // FIXME: confirm if data sent over datachannel and data sent over stream
	    // doesn't interface
	    while (buf.remaining() > 0)
	    {
	      SCToUse.write(buf);
	    } // SocketChannel writes may write less than requested
	  }

	  public int getSocketType()
	  {
	    return socketType;
	  }

	  public String getStringGUID()
	  {
	    return CommonMethods.bytArrayToHex(GUID);
	  }

	  public byte[] getByteGUID()
	  {
	    return GUID;
	  }*/

	  /**
	   * true for acquire, false for realease
	   * 
	   * @param acquireOrRelease
	   * @throws InterruptedException
	   */
	  /*public synchronized boolean grabLockOrgetBlocked(boolean acquireOrRelease)
	  {
	    // lock acquire case
	    if (acquireOrRelease)
	    {
	      if (lock == false)
	      {
	        lock = true;
	        return true;
	      }
	      else if (lock == true)
	      {

	        return false;
	      }
	    }
	    else if (!acquireOrRelease)
	    {
	      lock = false;
	      return true;
	    }
	    return false;
	  }

	  public synchronized boolean workingperations(int operationType, boolean value)
	  {
	    switch (operationType)
	    {
	      case GET :
	      {
	        return working;
	      }
	      case SET :
	      {
	        working = value;
	        break;
	      }
	    }
	    return false;
	  }

	  // private methods
	  private synchronized SetupControlMessage setupControlRead(SocketChannel SCToUse) throws IOException
	  {
	    ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);

	    while (buf.position() < SetupControlMessage.SIZE)
	    {
	      SCToUse.read(buf);
	    }
	    SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(buf.array());
	    return scm;
	  }*/

	  /**
	   * This function, splices the connection establishment process, reads setup
	   * control of client, for wards that setup control to server,
	   * 
	   * @param NewChannel
	   * @throws IOException
	   */
	  /*private void setupControlServer(SocketChannel NewChannel) throws IOException
	  {
	    SetupControlMessage scm = null;
	    scm = setupControlRead(NewChannel);

	    switch (scm.MesgType)
	    {
	      case SetupControlMessage.NEW_CON_MESG :
	      case SetupControlMessage.MIGRATE_SOCKET :
	      case SetupControlMessage.ADD_SOCKET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        byte[] ipAddress = new byte[4];
	        System.arraycopy(scm.GUID, 0, ipAddress, 0, 4);
	        
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        String stringGUID = CommonMethods.bytArrayToHex(scm.GUID);
	        

	        MSocketLogger.getLogger().fine("GUID used for querying, should be same as real hex rep of GUID " + stringGUID);

	        ProxyMSocket CtrlSocket = pForwarder.ProxyControlChannelMap(ProxyForwarder.GET, stringGUID, null);
	        int ProxyId = (Integer) pForwarder.spliceMapOperations(-1, ProxyTCPSplicer.PUT, ProxyTCPSplicer.CLIENT_SIDE,
	            this);
	        SocketChannel ControlChannel = CtrlSocket.getUnderlyingChannel();
	        MSocketLogger.getLogger().fine("Control Skt Remote IP" + ControlChannel.socket().getInetAddress() + " Remote Port "
	            + ControlChannel.socket().getPort());

	        // sending client's IP and port to the server, so the server opens a
	        // connection with this tuple, so that the proxy can splice the two ends
	        int OutMesgType = -1;
	        if (scm.MesgType == SetupControlMessage.NEW_CON_MESG)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type NEW_CON_MESG");
	          OutMesgType = SetupControlMessage.NEW_CON_REQ;
	        }
	        else if (scm.MesgType == SetupControlMessage.MIGRATE_SOCKET)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type MIGRATE_SOCKET");
	          OutMesgType = SetupControlMessage.MIGRATE_SOCKET_REQ;
	        }
	        else if (scm.MesgType == SetupControlMessage.ADD_SOCKET)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type ADD_SOCKET");
	          OutMesgType = SetupControlMessage.ADD_SOCKET_REQ;
	        }
	        // sending it using control socket, to synchronize sending of keep alive
	        // and new connection request
	        CtrlSocket.setupControlWrite(scm.iaddr, scm.port, scm.flowID, scm.ackSeq, OutMesgType,
	            scm.SocketId, ProxyId, scm.GUID, ControlChannel);

	        MSocketLogger.getLogger().fine("setupControl write done");

	        socketType = ProxyForwarder.DATA_SOC;
	        break;
	      }
	      case SetupControlMessage.NEW_CON_MESG_REPLY :
	      case SetupControlMessage.MIGRATE_SOCKET_REPLY :
	      case SetupControlMessage.ADD_SOCKET_REPLY :
	      case SetupControlMessage.MIGRATE_SOCKET_RESET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        // storing server ProxyMSOcekt in the splicer
	        pForwarder.spliceMapOperations(scm.ProxyId, ProxyForwarder.PUT, ProxyTCPSplicer.SERVER_SIDE, this);

	        // retrieving client ProxyMSOcket from the splicer to send control
	        // message
	        ProxyMSocket ClientProxyMSocket = (ProxyMSocket) pForwarder.SpliceMapOperations(scm.ProxyId,
	            ProxyForwarder.GET, ProxyTCPSplicer.CLIENT_SIDE, null);

	        // sending client's IP and port to the server, so the server opens a
	        // connection with this tuple, so that the proxy can splice the two ends
	        setupControlWrite(scm.iaddr, scm.port, scm.flowID, scm.ackSeq, scm.MesgType, scm.SocketId,
	            scm.ProxyId, scm.GUID, ClientProxyMSocket.getUnderlyingChannel());

	        socketType = ProxyForwarder.DATA_SOC;

	        if (scm.MesgType == SetupControlMessage.NEW_CON_MESG_REPLY)
	        {
	          ProxyLoadStatistics.addOpenTcpConn(1);
	        }
	        break;
	      }
	      case SetupControlMessage.CONTROL_SOCKET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        MSocketLogger.getLogger().fine("proxy sending CONTROL_SOCKET_REPLY");
	        this.setupControlWrite(NewChannel.socket().getLocalAddress(), -1, -1, -1,
	            SetupControlMessage.CONTROL_SOCKET_REPLY, -1, -1, scm.GUID, NewChannel);
	        socketType = ProxyForwarder.CONTROL_SOC;

	        MSocketLogger.getLogger().fine("Control channel with GUID " + GUID + " registered with proxy Server reigisters with proxy");
	        break;
	      }
	    }
	  }*/
	  
	  /**
	   * This function, splices the connection establishment process, reads setup
	   * control of client, for wards that setup control to server,
	   * 
	   * @param NewChannel
	   * @throws IOException
	   */
	 /* private void legacySetupControlServer(SocketChannel NewChannel) throws IOException
	  {
		  //long localFlowID = (long) (Math.random() * Long.MAX_VALUE);
		  
		  //SetupControlMessage scm = new SetupControlMessage(null, -1, localFlowID, 0, SetupControlMessage.NEW_CON_MESG,
		  //        1, -1, GUID);
		  //    ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());
			    
	    SetupControlMessage scm = null;
	    scm = setupControlRead(NewChannel);

	    switch (scm.MesgType)
	    {
	      case SetupControlMessage.NEW_CON_MESG :
	      case SetupControlMessage.MIGRATE_SOCKET :
	      case SetupControlMessage.ADD_SOCKET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        String stringGUID = CommonMethods.bytArrayToHex(scm.GUID);
	        

	        MSocketLogger.getLogger().fine("GUID used for querying, should be same as real hex rep of GUID " + stringGUID);

	        ProxyMSocket ctrlSocket = pForwarder.proxyControlChannelMap(ProxyForwarder.GET, stringGUID, null);
	        int ProxyId = (Integer) pForwarder.spliceMapOperations(-1, ProxyTCPSplicer.PUT, ProxyTCPSplicer.CLIENT_SIDE,
	            this);
	        SocketChannel ControlChannel = ctrlSocket.getUnderlyingChannel();
	        MSocketLogger.getLogger().fine("Control Skt Remote IP" + ControlChannel.socket().getInetAddress() + " Remote Port "
	            + ControlChannel.socket().getPort());

	        // sending client's IP and port to the server, so the server opens a
	        // connection with this tuple, so that the proxy can splice the two ends
	        int OutMesgType = -1;
	        if (scm.MesgType == SetupControlMessage.NEW_CON_MESG)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type NEW_CON_MESG");
	          OutMesgType = SetupControlMessage.NEW_CON_REQ;
	        }
	        else if (scm.MesgType == SetupControlMessage.MIGRATE_SOCKET)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type MIGRATE_SOCKET");
	          OutMesgType = SetupControlMessage.MIGRATE_SOCKET_REQ;
	        }
	        else if (scm.MesgType == SetupControlMessage.ADD_SOCKET)
	        {
	          MSocketLogger.getLogger().fine("GUID sent by client " + scm.GUID + " Client registered with proxy socketId " + scm.SocketId
	              + " mesg type ADD_SOCKET");
	          OutMesgType = SetupControlMessage.ADD_SOCKET_REQ;
	        }
	        // sending it using control socket, to synchronize sending of keep alive
	        // and new connection request
	        ctrlSocket.setupControlWrite(scm.iaddr, scm.port, scm.flowID, scm.ackSeq, OutMesgType,
	            scm.SocketId, ProxyId, scm.GUID, ControlChannel);

	        MSocketLogger.getLogger().fine("setupControl write done");

	        socketType = ProxyForwarder.DATA_SOC;
	        break;
	      }
	      case SetupControlMessage.NEW_CON_MESG_REPLY :
	      case SetupControlMessage.MIGRATE_SOCKET_REPLY :
	      case SetupControlMessage.ADD_SOCKET_REPLY :
	      case SetupControlMessage.MIGRATE_SOCKET_RESET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        // storing server ProxyMSOcekt in the splicer
	        pForwarder.spliceMapOperations(scm.ProxyId, ProxyForwarder.PUT, ProxyTCPSplicer.SERVER_SIDE, this);

	        // retrieving client ProxyMSOcket from the splicer to send control
	        // message
	        ProxyMSocket ClientProxyMSocket = (ProxyMSocket) pForwarder.SpliceMapOperations(scm.ProxyId,
	            ProxyForwarder.GET, ProxyTCPSplicer.CLIENT_SIDE, null);

	        // sending client's IP and port to the server, so the server opens a
	        // connection with this tuple, so that the proxy can splice the two ends
	        setupControlWrite(scm.iaddr, scm.port, scm.flowID, scm.ackSeq, scm.MesgType, scm.SocketId,
	            scm.ProxyId, scm.GUID, ClientProxyMSocket.getUnderlyingChannel());

	        socketType = ProxyForwarder.DATA_SOC;

	        if (scm.MesgType == SetupControlMessage.NEW_CON_MESG_REPLY)
	        {
	          ProxyLoadStatistics.addOpenTcpConn(1);
	        }
	        break;
	      }
	      case SetupControlMessage.CONTROL_SOCKET :
	      {
	        // setting GUID
	        System.arraycopy(scm.GUID, 0, GUID, 0, SetupControlMessage.SIZE_OF_GUID);
	        MSocketLogger.getLogger().fine("Sent GUID " + scm.GUID + " String form " + CommonMethods.bytArrayToHex(scm.GUID));

	        MSocketLogger.getLogger().fine("proxy sending CONTROL_SOCKET_REPLY");
	        this.setupControlWrite(NewChannel.socket().getLocalAddress(), -1, -1, -1,
	            SetupControlMessage.CONTROL_SOCKET_REPLY, -1, -1, scm.GUID, NewChannel);
	        socketType = ProxyForwarder.CONTROL_SOC;

	        MSocketLogger.getLogger().fine("Control channel with GUID " + GUID + " registered with proxy Server reigisters with proxy");
	        break;
	      }
	    }
	  }*/
	  
}