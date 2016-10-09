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
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements a timer to retransmit a message over a UDP channel.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class RetransmitTask extends TimerTask
{

  // max number of retransmit done for a message
  private static final int          MAX_RETRANSMIT   = 5;
  private ControlMessage            cmsg             = null;
  private FlowIDToControllerMapping Mapping          = null;
  private MServerSocketController   servercontroller = null;
  private ConnectionInfo            cinfo            = null;

  private int                       txCount          = 0;
  private Timer                     timer;

  /**
   * in the constructor invocation either servercontroller is null or Mapping is
   * null, used for both server controller and client UDP controller,
   * 
   * @param m
   * @param Mapping
   * @param servercontroller
   * @param cinfo
   * @throws IOException
   */
  RetransmitTask(ControlMessage m, FlowIDToControllerMapping Mapping, MServerSocketController servercontroller,
      ConnectionInfo cinfo) throws IOException
  {
    cmsg = m;
    this.Mapping = Mapping;
    this.servercontroller = servercontroller;
    this.cinfo = cinfo;
    timer = new Timer();
    if (m.getType() != ControlMessage.ACK_ONLY)
    {
      /*
       * Regular messages are scheduled for periodic retransmission until
       * acknowledged.
       */

      timer.schedule(this, 0, 1000);
    }
    else
    {
      // Acks are transmitted just once.
      timer.schedule(this, 0);
    }
  }

  /**
	 * 
	 */
  public void run()
  {
    try
    {

      if (cmsg.getType() == ControlMessage.ACK_ONLY)
      {
        MSocketLogger.getLogger().fine("Sending ack " + cmsg);
        retransmit();
      }
      else if (cinfo.getCtrlBaseSeq() <= cmsg.getSendseq())
      {
        if (txCount > 0)
          MSocketLogger.getLogger().fine("Retransmitting message " + cmsg + " coz baseseq=" + cinfo.getCtrlBaseSeq());
        retransmit();
        txCount++;
        if (txCount > MAX_RETRANSMIT)
        {
          timer.cancel();
        }
      }
      else
      {
        MSocketLogger.getLogger().fine("Completed delivery of message " + cmsg);
        timer.cancel();
      }
    }
    catch (IOException e)
    {
      MSocketLogger.getLogger().fine("IOException while retransmitting packet " + cmsg + "; canceling retransmission attempts");
      timer.cancel();
      e.printStackTrace();
    }
  }

  // private methods

  /**
   * @return
   * @throws IOException
   */
  private byte[] toByteArray() throws IOException
  {
    return cmsg.getBytes();
  }

  /**
   * @throws IOException
   */
  private void retransmit() throws IOException
  {
    DatagramPacket p = new DatagramPacket(toByteArray(), 0, toByteArray().length);
    InetSocketAddress sockaddr = new InetSocketAddress(cinfo.getRemoteControlAddress(), cinfo.getRemoteControlPort());
    p.setSocketAddress(sockaddr);

    // uses same retransmit class for both type of server socket controller and
    // client side FlowToIDConroller
    if (Mapping != null)
      Mapping.datagramSend(p);
    else if (servercontroller != null)
    {
      servercontroller.datagramSend(p);
    }
  }
}