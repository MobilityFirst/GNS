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
import java.util.HashMap;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements a UDP controller for an interface on the client side.
 * On the client side, there can be one UDP controller per interface. The class
 * also keeps a mapping of all the MSocket clients that use this UDP controller.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FlowIDToControllerMapping implements Runnable
{

  /**
   * Keeps the flowID to contrller mapping in single controller class
   * 
   * @author ayadav
   */

  public static final int               NAT_KEEP_ALIVE_INTERVAL = 5000;

  private HashMap<Long, ConnectionInfo> connectionInfoMap       = null;
  private DatagramSocket                udpSocket               = null;

  private boolean                       isClosed                = false;
  private KeepAliveThread               kat                     = null;

  public class KeepAliveThread implements Runnable
  {
    FlowIDToControllerMapping Obj = null;

    KeepAliveThread(FlowIDToControllerMapping Obj)
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
      MSocketLogger.getLogger().fine("FlowIDToControllerMapping KeepAliveThread threads exits");
    }
  }

  /**
   * @throws SocketException
   */
  public FlowIDToControllerMapping(InetAddress UDPSocketAddress) throws SocketException
  {
    connectionInfoMap = new HashMap<Long, ConnectionInfo>();
    udpSocket = new DatagramSocket(0, UDPSocketAddress);
    // udpSocket.setSoTimeout(NAT_KEEP_ALIVE_INTERVAL);
    kat = new KeepAliveThread(this);
  }

  public void startKeepAliveThread()
  {
    (new Thread(kat)).start();
  }

  public void closeUDPController()
  {
    isClosed = true;
    if (udpSocket != null)
    {
      udpSocket.close();
    }
  }

  /**
   * @return
   */
  public void addControllerMapping(Long flowID, ConnectionInfo cinfo)
  {
    connectionInfoMap.put(flowID, cinfo);
  }

  /**
   * @param flowID
   * @param cinfo
   */
  public ConnectionInfo removeControllerMapping(Long flowID)
  {
    // FIXME: see if any other thing is required while switching cinfo form one
    // UDP to another
    return connectionInfoMap.remove(flowID);
  }

  /**
   * @return
   */
  public int getLocalPort()
  {
    return udpSocket.getLocalPort();
  }

  /**
   * @return
   */
  public InetAddress getLocalAddress()
  {
    return udpSocket.getLocalAddress();
  }

  /**
   * @param flowID
   * @return
   */
  public ConnectionInfo getConnectionInfo(long flowID)
  {
    return connectionInfoMap.get(flowID);

  }

  /**
   * @param p
   * @throws IOException
   */
  public void datagramSend(DatagramPacket p) throws IOException
  {
    try
    {
      udpSocket.send(p);
    }
    catch (IOException ex)
    {
      MSocketLogger.getLogger().fine("Exception in DatagramSend");
    }
  }

  /**
   * @param flowID
   * @param Mesg_Type
   * @return
   * @throws IOException
   */
  public synchronized int sendControllerMesg(long flowID, int Mesg_Type) throws IOException
  {
    ConnectionInfo cinfo = connectionInfoMap.get(flowID);
    if (cinfo == null)
    {
      MSocketLogger.getLogger().fine("Unregistered flowID message recv ");
      return 0;
    }

    switch (Mesg_Type)
    {
      case ControlMessage.ACK_ONLY :
      {
        ControlMessage ack = new ControlMessage(cinfo.getCtrlSendSeq(), cinfo.getCtrlAckSeq(), ControlMessage.ACK_ONLY,
            flowID);
        send(ack);
        break;
      }
    }
    return 0;
  }

  /**
   * @throws IOException
   */
  public void sendKeepAliveOnAllConnections() throws IOException
  {
    for (Object obj : connectionInfoMap.values())
    {
      // UDP keep alive for NAT bindings
      ConnectionInfo ci = (ConnectionInfo) obj;

      ControlMessage cmsg = new ControlMessage
    		  (-1, -1, ControlMessage.KEEP_ALIVE, ci.getConnID());

      DatagramPacket p = new DatagramPacket(cmsg.getBytes(), 0, 
    		  	cmsg.getBytes().length);
      
      InetSocketAddress sockaddr = new InetSocketAddress(ci.getRemoteControlAddress(), 
    		  ci.getRemoteControlPort());
      p.setSocketAddress(sockaddr);
      datagramSend(p);
    }
  }

  @Override
  public void run()
  {
    try
    {
      MSocketLogger.getLogger().fine("Controller: " + "[ " + getLocalPort() + ", " + getLocalAddress() + "]");
      while (true)
      {
        process(receiveControlMessage());
        if (isClosed)
          break;
      }
    }
    catch (Exception e)
    {
      MSocketLogger.getLogger().fine(e.toString());
    }
    MSocketLogger.getLogger().fine("FlowIDToControllerMapping UDP recv thread exits");
  }

  /**
   * @param msg
   * @throws IOException
   */
  private void send(ControlMessage msg) throws IOException
  {
    new RetransmitTask(msg, this, null, getConnectionInfo(msg.getFlowID()));
  }

  /**
   * @return
   * @throws IOException
   */
  private ControlMessage receiveControlMessage() throws IOException
  {
    return extract(receive());
  }

  /**
   * @return
   * @throws IOException
   */
  private DatagramPacket receive() throws IOException
  {
    byte[] buf = new byte[256];
    DatagramPacket p = new DatagramPacket(buf, 0, buf.length);

    udpSocket.receive(p);

    return p;
  }

  /**
   * @param buf
   * @return
   * @throws IOException
   */
  private ControlMessage toControlMessage(byte[] buf) throws IOException
  {
    ControlMessage msg = null;
    msg = ControlMessage.getControlMessage(buf);
    return msg;
  }

  /**
   * @param p
   * @return
   */
  private ControlMessage extract(DatagramPacket p)
  {
    byte[] buf = p.getData();
    ControlMessage msg = null;
    try
    {
      msg = toControlMessage(buf);
    }
    catch (IOException e)
    {
      MSocketLogger.getLogger().fine("IOException while processing received message; discarding message");
      e.printStackTrace();
    }
    return msg;
  }

  /**
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void process(ControlMessage msg) throws IOException, InterruptedException
  {
    if (msg.getType() == ControlMessage.KEEP_ALIVE)
    {
      return;
    }
    // modified here from != to > as if ACK got lost, then sender will resend
    // that message. but here just ACK needs to be sent
    ConnectionInfo cinfo = connectionInfoMap.get(msg.getFlowID());
    if (cinfo == null)
    {
      MSocketLogger.getLogger().fine("Unregistered flowID message recv ");
      return;
    }

    if (msg.sendseq > cinfo.getCtrlAckSeq())
    {
      MSocketLogger.getLogger().fine("Received out-of-order message " + msg + "; expecting ackseq=" + cinfo.getCtrlAckSeq());
      return;
    }
    else
    {
      MSocketLogger.getLogger().fine("Received in-order message " + msg);
    }

    if (msg.type == ControlMessage.REBIND_ADDRESS_PORT)
    {
      MSocketLogger.getLogger().fine("Got Rebind");
      if (msg.sendseq == cinfo.getCtrlAckSeq()) // for the case when ACK of
                                                // rebind gets lost, so the
                                                // server will send rebind
                                                // again, but no rebind should
                                                // happen only ACK should be
                                                // sent
      {
        // need to set remote control ser ip and port as it may also have
        // changed during serve migration
        cinfo.setRemoteControlAddress(msg.getInetAddress());
        cinfo.setRemoteControlPort(msg.getRemoteUDPControlPort());
        MSocketLogger.getLogger().fine("REBIND_ADDRESS_PORT UDP port is " + msg.getRemoteUDPControlPort());

        boolean domigrate = true;

        if (domigrate)
        {
          InetSocketAddress currSockAddr = new InetSocketAddress(cinfo.getServerIP(), 
      											cinfo.getServerPort());;

          if (currSockAddr.equals(new InetSocketAddress(msg.getInetAddress(), msg.getPort())))
          {
            MSocketLogger.getLogger().fine
            	("Already connected to new address, no need for server intiated mig");
          }
          else
          {
            cinfo.setMigrateRemote(true);
            cinfo.closeAll();
            cinfo.migrateRemote(msg.getInetAddress(), msg.getPort());
            cinfo.setMigrateRemote(false);
          }
        }
      }
    }
    else if (msg.type == ControlMessage.ACK_ONLY)
    {
      MSocketLogger.getLogger().fine("ACK recv " + msg.getAckseq());
      if (msg.getAckseq() > cinfo.getCtrlBaseSeq())
      {
        cinfo.setCtrlBaseSeq(msg.getAckseq());
      }
    }
    if (msg.getType() != ControlMessage.ACK_ONLY)
    { // FIXME: need to find that if this message had previously arrived and our
      // ACK just got lost so that we don't do REBINDING again

      MSocketLogger.getLogger().fine("sending ACK msg.sendseq" + msg.sendseq);
      cinfo.setCtrlAckSeq(msg.sendseq + 1);
      sendControllerMesg(msg.getFlowID(), ControlMessage.ACK_ONLY);
    }
  }
}