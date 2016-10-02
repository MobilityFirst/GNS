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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.gns.Integration;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the internals of MServerSocket. It implements the UDP
 * controller. Each server has it's own UDP controller. This class also
 * maintains the proxyMap and other MServerSocket related data structures
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class MServerSocketController implements Runnable
{
  /**
   * 2 mins
   */
  private static final int              NAT_KEEP_ALIVE_INTERVAL = 1000;

  private final Object                  proxyMapMonitor         = new Object();

  /**
   * TImer ticks after 1000msec
   */
  private static final int              TimerTick               = 1000;

  /**
   * proxy failure assumed after 10 secs
   */
  private static final int              proxyFailureTimeout     = 15;

  /**
   * Storing proxy info
   */
  private HashMap<String, ProxyInfo>    proxyMap                = null;

  private KeepAliveThread               kat                     = null;

  private DatagramSocket                ctrlSocket              = null;
  private DatagramSocket                oldCtrlSocket           = null;
  private MServerSocket                 mserversocket           = null;
  /**
   * ConnectionInfo of all the accepted sockets
   */
  private HashMap<Long, ConnectionInfo> cinfoMap                = null;
  private ConnectionToProxyServer       proxyConnecObj          = null;

  private boolean                       isClosed                = false;

  private long                          localClock              = 0;            // incremented
                                                                                 // on
                                                                                 // timer
  private Timer                         localTimer              = null;         // for
                                                                                 // keep
                                                                                 // alive
                                                                                 // and
                                                                                 // retransmission
                                                                                 // of
                                                                                 // last
                                                                                 // unACK
                                                                                 // data

  private final Object                  cinfoMapOprMonitor      = new Object();

  /**
   * Creates a new <code>MServerSocketController</code> object
   * 
   * @param ms
   * @throws SocketException
   */
  public MServerSocketController(MServerSocket ms) throws SocketException
  {
    mserversocket = ms;
    MSocketLogger.getLogger().fine(mserversocket.getInetAddress().toString());
    ctrlSocket = new DatagramSocket(0, mserversocket.getInetAddress());
    cinfoMap = new HashMap<Long, ConnectionInfo>();
    kat = new KeepAliveThread(this);

    proxyMap = new HashMap<String, ProxyInfo>();

    localTimer = new Timer();
    startLocalTimer();
  }

  Collection<ProxyInfo> getAllProxyInfo()
  {
    synchronized (proxyMapMonitor)
    {
      return proxyMap.values();
    }
  }

  void setProxyInfo(String key, ProxyInfo pi)
  {
    synchronized (proxyMapMonitor)
    {
      proxyMap.put(key, pi);
    }
  }

  void removeProxyInfo(String key)
  {
    synchronized (proxyMapMonitor)
    {
      proxyMap.remove(key);
    }
  }

  void setConnectionInfo(long connID)
  {
    ConnectionInfo cinfo = getConnectionInfo(connID);
    
    if (cinfo == null)
    {
      cinfo = new ConnectionInfo(connID, this);
      setConnectionInfo(connID, cinfo);
    }
  }

  public ConnectionInfo getConnectionInfo(Long flowId)
  {
    synchronized (cinfoMapOprMonitor)
    {
      return cinfoMap.get(flowId);
    }
  }
  
  // FIXME: here previous socket address may not be used, use new IP if
  // server migrated and send this info to cliet as well
  int renewControlSocket(InetAddress bindToMe) throws SocketException
  {
    // no need for new socket
    if (ctrlSocket.getLocalAddress().equals(bindToMe))
      return ctrlSocket.getLocalPort();

    oldCtrlSocket = ctrlSocket;
    oldCtrlSocket.close();
    // bind UDP to new socket as well

    ctrlSocket = new DatagramSocket(0, bindToMe);

    return ctrlSocket.getLocalPort();
  }

  void send(ControlMessage msg) throws IOException
  {
    new RetransmitTask(msg, null, this, getConnectionInfo(msg.getFlowID()));
  }

  void datagramSend(DatagramPacket p)
  {
    try
    {
      ctrlSocket.send(p);
    }
    catch (IOException ex)
    {
      MSocketLogger.getLogger().fine("IO Exception caused while sending udp keep alive");
    }
  }

  void initMigrateChildren(InetAddress iaddr, int port, int UDPPort) throws IOException
  {
    Vector<ConnectionInfo> allConnections = new Vector<ConnectionInfo>();
    allConnections.addAll(getAllConnectionInfo());

    for (int i = 0; i < allConnections.size(); i++)
    {

      ConnectionInfo ci = allConnections.get(i);
      MSocketLogger.getLogger().fine("Initiating migrate for flow " + ci.getConnID());
      // Prepare control message

      initMigrate(iaddr, port, ci.getConnID(), UDPPort);

      // Thread.sleep(10);
      // ci.notifyAll();
    }
  }

  void closeChildren() throws IOException
  {
    Vector<ConnectionInfo> allConnections = new Vector<ConnectionInfo>();
    allConnections.addAll(getAllConnectionInfo());
    for (int i = 0; i < allConnections.size(); i++)
    {
      ConnectionInfo ci = allConnections.get(i);
      MSocketLogger.getLogger().fine("closing prev socket for flow " + ci.getConnID());
      this.suspendIO(ci.getConnID());
    }
  }

  private void initMigrate(InetAddress iaddr, int port, long connID, int udpPort) 
		  throws IOException
  {
    this.sendControllerMesg(connID, ControlMessage.REBIND_ADDRESS_PORT, udpPort, port, iaddr);
  }

  private synchronized int sendControllerMesg
  	(long connID, int Mesg_Type, int UDPPort, int port, InetAddress iaddr)
      throws IOException
  {
    ConnectionInfo cinfo = getConnectionInfo(connID);
    switch (Mesg_Type)
    {
      case ControlMessage.ACK_ONLY :
      {
        ControlMessage ack = new ControlMessage(cinfo.getCtrlSendSeq(), 
        		cinfo.getCtrlAckSeq(), ControlMessage.ACK_ONLY, connID);
        send(ack);
        break;
      }
      case ControlMessage.REBIND_ADDRESS_PORT :
      {
        ControlMessage cmsg = new ControlMessage(cinfo.getCtrlSendSeq(), cinfo.getCtrlAckSeq(),
            ControlMessage.REBIND_ADDRESS_PORT, connID, port, UDPPort, iaddr);
        MSocketLogger.getLogger().fine("Sending control message " + cmsg.toString() 
        	+ " to " + cinfo.getRemoteControlAddress() + ":"
            + cinfo.getRemoteControlPort());
        send(cmsg);
        cinfo.setCtrlSendSeq(cinfo.getCtrlSendSeq() + 1);

        break;
      }
    }
    return 0;
  }
  
  public DatagramSocket getDatagramSocket()
  {
    return ctrlSocket;
  }

  int getLocalPort()
  {
    return ctrlSocket.getLocalPort();
  }

  InetAddress getLocalAddress()
  {
    return ctrlSocket.getLocalAddress();
  }

  void setProxyConnObj(ConnectionToProxyServer ProxyConnecObj)
  {
    this.proxyConnecObj = ProxyConnecObj;
  }

  ConnectionToProxyServer getProxyConnObj()
  {
    return this.proxyConnecObj;
  }

  void close()
  {
    isClosed = true;
    // close the selector associated
    if (proxyConnecObj != null)
    {
      proxyConnecObj.closeProxyConnection();
    }
    if (ctrlSocket != null)
      ctrlSocket.close();
  }

  /**
   * Returns true if the server socket has been closed
   * 
   * @return
   */
  public boolean isClosed()
  {
    return isClosed;
  }

  public void run()
  {
    try
    {
      while (!isClosed)
      {
        DatagramPacket dg = receive();
        if (dg == null)
          continue;
        process(extract(dg));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    MSocketLogger.getLogger().fine("MServerSocketController UDP thread exits");
  }

  long getLocalClock()
  {
    return localClock;
  }

  /**
   * returns a non loop back IPv4 interface address on the proxy to start
   * listening on
   * 
   * @return
   */

  // private methods

  // protocol is ignore the out of order message, send the ACK for the
  // resent message, basically receiving window is 1 here
  private void process(ControlMessage msg) throws IOException
  {
    if (msg == null)
      return;
    if (msg.getType() == ControlMessage.KEEP_ALIVE)
    {
      return;
    }
    ConnectionInfo cinfo = getConnectionInfo(msg.getFlowID());

    // modified here from != to > as if ACK got lost, then sender will
    // resend that message. but here just ACK needs to be sent
    if (msg.sendseq > cinfo.getCtrlAckSeq())
    {
      MSocketLogger.getLogger().fine("Received out-of-order message " + msg + "; expecting ackseq=" + cinfo.getCtrlAckSeq());
      return;
    }
    else
    {
      MSocketLogger.getLogger().fine("Received in-order message " + msg);
    }

    if (msg.type == ControlMessage.ACK_ONLY)
    {
      MSocketLogger.getLogger().fine("ACK recv " + msg.getAckseq());
      // changed added this condition
      if (msg.getAckseq() > cinfo.getCtrlBaseSeq())
      {
        cinfo.setCtrlBaseSeq(msg.getAckseq());
      }
    }
    if (msg.getType() != ControlMessage.ACK_ONLY)
    { // FIXME: need to find that if this message had previously arrived
      // and our ACK just got lost so that we don't do REBINDING again

      if (msg.sendseq == cinfo.getCtrlAckSeq())
      {
      }

      MSocketLogger.getLogger().fine("sending ACK msg.sendseq" + msg.sendseq);
      cinfo.setCtrlAckSeq(msg.sendseq + 1);
      sendControllerMesg(msg.getFlowID(), ControlMessage.ACK_ONLY, 0, 0, null);
    }
  }

  private void startLocalTimer()
  {
    localTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        if (isClosed)
        {
          localTimer.cancel();
          return;
        }

        localClock++;
        if (mserversocket.getProxySelection().hasAvailableProxies())
        {
          try
          {
            proxyFailureCheck();
          }
          catch (IOException e)
          {
            e.printStackTrace();
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
        }
      }
    }, TimerTick, TimerTick);
  }

  private void proxyFailureCheck() throws Exception
  {
    Vector<ProxyInfo> vect = new Vector<ProxyInfo>();
    vect.addAll(getAllProxyInfo());
    int i = 0;
    boolean proxyFailure = false;
    while (i < vect.size())
    {
      ProxyInfo value = vect.get(i);
      {
        if (((localClock - value.getLastKeepAlive()) > proxyFailureTimeout) && value.getActive())
        {
          MSocketLogger.getLogger().fine("proxy Name" + value.getProxyName() + "proxy Port " + value.getProxyPort() + "last Keep alive "
              + value.getLastKeepAlive() + "current clock " + localClock);
          value.setActive(false);
          proxyFailure = true;
        }
      }
      i++;
    }
    if (proxyFailure)
    {
      // do this only when at least one interface is active
      if (CommonMethods.getActiveInterfaceInetAddresses().size() > 0)
      {
        handleProxyFailure();
      }
    }
  }

  /**
   * update GNS,
   * 
   * @throws Exception
   */
  private void handleProxyFailure() throws Exception
  {
    Vector<ProxyInfo> vect = new Vector<ProxyInfo>();
    vect.addAll(getAllProxyInfo());
    String GNRSValue = "";
    for (int i = 0; i < vect.size(); i++)
    {
      ProxyInfo Obj = vect.get(i);
      if (Obj.getActive())
      {
        if (GNRSValue.length() > 0)
          GNRSValue += ",";
        GNRSValue += Obj.getProxyInfo();
      }
      else
      {
        String ProxyInfo = Obj.getProxyInfo();
        String[] Parsed = ProxyInfo.split(":");
        InetSocketAddress badProxyAdd = new InetSocketAddress(Parsed[0], Integer.parseInt(Parsed[1]));

        try
        {
          getProxyConnObj().removeProxy(Obj.getProxyInfo(), Obj);
          Integration.unregisterWithGNS(mserversocket.getServerName(), badProxyAdd);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    }

    // no active proxy remaining add a new one
    if (proxyMap.size() == 0)
    {
      InetSocketAddress retProxy = Integration.getNewProxy(mserversocket.getProxySelection()).get(0);

      Integration.registerWithGNS(mserversocket.getServerName(), retProxy);

      ProxyInfo Obj = new ProxyInfo(retProxy.getHostName(), retProxy.getPort());
      // just setting it to current time
      Obj.setLastKeepAlive(getLocalClock());
      Obj.setActive(true);
      getProxyConnObj().addProxy(Obj.getProxyInfo(), Obj);
    }
  }

  private DatagramPacket receive() throws Exception
  {
    byte[] buf = new byte[256];
    DatagramPacket p = new DatagramPacket(buf, 0, buf.length);
    try
    {
      ctrlSocket.receive(p);
    }
    catch (Exception e)
    {
      return null;
    }
    return p;
  }

  private ControlMessage toControlMessage(byte[] buf) throws IOException
  {
    return ControlMessage.getControlMessage(buf);
  }

  private ControlMessage extract(DatagramPacket p) throws Exception
  {
    ControlMessage msg = null;

    byte[] buf = p.getData();
    InetAddress NATedAddress = p.getAddress();
    int NATedPort = p.getPort();

    try
    {
      msg = toControlMessage(buf);

      // setting NAT UDP address after getting any UDP message

      ConnectionInfo cinfo = getConnectionInfo(msg.getFlowID());
      if (cinfo == null)
      {
        MSocketLogger.getLogger().fine("cinfo for flowID " + msg.getFlowID() + " not found, " + "behold the nullpointer expception");
        return null;
      }
      InetAddress KnownIP = cinfo.getRemoteControlAddress();
      int KnownPort = cinfo.getRemoteControlPort();
      if (KnownIP.equals(NATedAddress) && (KnownPort == NATedPort))
      {

      }
      else
      {
        MSocketLogger.getLogger().fine("Remote UDP IP set to " + NATedAddress.getHostAddress() + " port set to" + NATedPort);
        cinfo.setRemoteControlAddress(NATedAddress);
        cinfo.setRemoteControlPort(NATedPort);
      }
    }
    catch (IOException e)
    {
      MSocketLogger.getLogger().fine("IOException while processing received message; discarding message");
      e.printStackTrace();
    }
    return msg;
  }

  private void suspendIO(long flowID) throws IOException
  {
    ConnectionInfo cinfo = getConnectionInfo(flowID);

    cinfo.closeAll();
  }

  private Collection<ConnectionInfo> getAllConnectionInfo()
  {
    synchronized (cinfoMapOprMonitor)
    {
      return cinfoMap.values();
    }
  }

  private void setConnectionInfo(Long connID, ConnectionInfo ci)
  {
    synchronized (cinfoMapOprMonitor)
    {
      cinfoMap.put(connID, ci);
    }
  }

  ConnectionInfo removeConnectionInfo(Long connID)
  {
    synchronized (cinfoMapOprMonitor)
    {
      MSocketLogger.getLogger().fine(" flowID " + connID + " removed from cinfoMap");
      ConnectionInfo removed = cinfoMap.remove(connID);
      return removed;
    }
  }

  // // KEEPALIVE THREAD

  private class KeepAliveThread implements Runnable
  {
    MServerSocketController Obj = null;

    KeepAliveThread(MServerSocketController Obj)
    {
      this.Obj = Obj;
    }

    @Override
    public void run()
    {
      while (!isClosed)
      {
        try
        {
          Obj.sendKeepAliveOnAllConnections();
          Thread.sleep(NAT_KEEP_ALIVE_INTERVAL);
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      MSocketLogger.getLogger().fine("MServerSocketController KeepAliveThread exits");
    }
  }

  /**
   * Returns the underlying MServerSocket
   * 
   * @return
   */
  public MServerSocket getMServerSocket()
  {
    return mserversocket;
  }

  void startKeepAliveThread()
  {
    (new Thread(kat)).start();
  }

  private void sendKeepAliveOnAllConnections() throws IOException
  {
    Vector<ConnectionInfo> cinfoVector = new Vector<ConnectionInfo>();
    cinfoVector.addAll(getAllConnectionInfo());
    int i = 0;
    for (i = 0; i < cinfoVector.size(); i++)
    {
      ConnectionInfo ci = cinfoVector.get(i);

      if (ci.getRemoteControlPort() == -1)
      {
        continue;
      }

      ControlMessage cmsg = new ControlMessage(-1, -1, ControlMessage.KEEP_ALIVE, 
    		  ci.getConnID());

      DatagramPacket p = new DatagramPacket(cmsg.getBytes(), 0, cmsg.getBytes().length);
      InetSocketAddress sockaddr = new InetSocketAddress(ci.getRemoteControlAddress(), ci.getRemoteControlPort());
      p.setSocketAddress(sockaddr);
      datagramSend(p);

    }
  }

}