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
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the setup control message format, serialization and
 * de-serialization. Setup control messages are sent in the beginning to
 * establish the connection.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class SetupControlMessage
{
  // client sends these
  public static final int  NEW_CON_MESG         = 1;                  // mainly
                                                                          // negotiation
                                                                          // of
                                                                          // number
                                                                          // of
                                                                          // socekts
                                                                          // in
                                                                          // multipath,
                                                                          // which
                                                                          // is
                                                                          // currently
                                                                          // not
                                                                          // here
                                                                          // much
  public static final int  ADD_SOCKET           = 2;                     // Add
                                                                          // one
                                                                          // more
                                                                          // socket
                                                                          // to
                                                                          // this
                                                                          // flow,
                                                                          // each
                                                                          // socket
                                                                          // is
                                                                          // identfied
                                                                          // by
                                                                          // a
                                                                          // socket
                                                                          // id
                                                                          // same
                                                                          // at
                                                                          // both
                                                                          // client
                                                                          // and
                                                                          // server
  public static final int  MIGRATE_SOCKET       = 3;                     // migrates
                                                                          // socket
                                                                          // with
                                                                          // mentioned
                                                                          // flowID,
                                                                          // replaces
                                                                          // old
                                                                          // socket
                                                                          // in
                                                                          // MSocket
                                                                          // with
                                                                          // newly
                                                                          // accepted
                                                                          // socket

  // server sends these
  public static final int  NEW_CON_MESG_REPLY   = 4;                     // mainly
                                                                          // negotiation
                                                                          // of
                                                                          // number
                                                                          // of
                                                                          // sockets
                                                                          // in
                                                                          // multipath,
                                                                          // which
                                                                          // is
                                                                          // currently
                                                                          // not
                                                                          // here
                                                                          // much
  public static final int  ADD_SOCKET_REPLY     = 5;                     // Add
                                                                          // one
                                                                          // more
                                                                          // socket
                                                                          // to
                                                                          // this
                                                                          // flow,
                                                                          // each
                                                                          // socket
                                                                          // is
                                                                          // identfied
                                                                          // by
                                                                          // a
                                                                          // socket
                                                                          // id
                                                                          // same
                                                                          // at
                                                                          // both
                                                                          // client
                                                                          // and
                                                                          // server
  public static final int  MIGRATE_SOCKET_REPLY = 6;                     // migrates
                                                                          // socket
                                                                          // with
                                                                          // mentioned
                                                                          // flowID,
                                                                          // replaces
                                                                          // old
                                                                          // socket
                                                                          // in
                                                                          // MSocket
                                                                          // with
                                                                          // newly
                                                                          // accepted
                                                                          // socket

  public static final int  CONTROL_SOCKET       = 7;                     // control
                                                                          // socket
                                                                          // that
                                                                          // server
                                                                          // open
                                                                          // with
                                                                          // proxy
  public static final int  CONTROL_SOCKET_REPLY = 8;                     // reply

  public static final int  NEW_CON_REQ          = 9;                     // control
                                                                          // socket
                                                                          // that
                                                                          // server
                                                                          // open
                                                                          // with
                                                                          // proxy

  public static final int  MIGRATE_SOCKET_REQ   = 10;
  public static final int  ADD_SOCKET_REQ       = 11;
  public static final int  KEEP_ALIVE           = 12;                    // keep
                                                                          // alive
                                                                          // message
                                                                          // on
                                                                          // conroller
                                                                          // channel
                                                                          // to
                                                                          // server

  public static final int  MIGRATE_SOCKET_RESET = 13;
  

  public static final int  SIZE_OF_GUID         = 20;                     // 20
                                                                          // bytes
                                                                          // is
                                                                          // the
                                                                          // size
                                                                          // of
                                                                          // GUID, right coversion requires 1 more byte, from 20 to 20

  public static final int  SIZE                 = ControlMessage.INET_ADDR_SIZE + (Integer.SIZE * 5 + Long.SIZE) / 8
                                                    + SIZE_OF_GUID;

  public final InetAddress iaddr;                                        // src
                                                                          // ip
  public final int         port;                                         // udp
                                                                          // controller
                                                                          // port
  public final long        connID;
  public final int         ackSeq;
  public final int         mesgType;
  public final int         socketID;

  public final int         proxyID;                                      // ID
                                                                          // of
                                                                          // the
                                                                          // connection
                                                                          // at
                                                                          // proxy,
                                                                          // generated
                                                                          // by
                                                                          // proxy

  public final byte[]      GUID                 = new byte[SIZE_OF_GUID];

  public SetupControlMessage(InetAddress ia, int p, long connid, 
		  int as, int mstype, int socketid,
      int proxyid, byte[] guid)
  {
    this.iaddr = ia;
    this.port = p;
    this.connID = connid;
    this.ackSeq = as;
    this.mesgType = mstype;
    this.socketID = socketid;
    
    this.proxyID = proxyid;
    System.arraycopy(guid, 0, this.GUID, 0, SIZE_OF_GUID);
  }

  public byte[] getBytes() throws UnknownHostException
  {
    ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);
    buf.put(this.iaddr.getAddress());
    buf.putInt(this.port);
    buf.putLong(this.connID);
    buf.putInt(this.ackSeq);
    buf.putInt(this.mesgType);
    buf.putInt(this.socketID);
    buf.putInt(this.proxyID);
    buf.put(this.GUID, 0, SIZE_OF_GUID);
    buf.flip();
    return buf.array();
  }

  public static SetupControlMessage getSetupControlMessage(byte[] b) throws UnknownHostException
  {
    if (b == null)
      return null;
    ByteBuffer buf = ByteBuffer.wrap(b);
    byte[] ia = new byte[ControlMessage.INET_ADDR_SIZE];
    buf.get(ia);
    int port = buf.getInt();
    long connID = buf.getLong();
    int ackSeq = buf.getInt();
    int mesgType = buf.getInt();
    int socketId = buf.getInt();
    int proxyId = buf.getInt();
    byte[] GUID = new byte[SIZE_OF_GUID];
    buf.get(GUID);

    SetupControlMessage cm = new SetupControlMessage(InetAddress.getByAddress(ia), port, 
    		connID, ackSeq, mesgType,
       socketId, proxyId, GUID);
    return cm;
  }

  public String toString()
  {
    String s = "[" + iaddr + ", " + port + ", " + connID + "]";
    return s;
  }
  
  /**
   * Reads setup control message from a channel
   * Synchronization of this methid is caller's responsibility
   * @param scToUse
   * @return
   * @throws IOException
   */
  public static SetupControlMessage setupControlRead(SocketChannel scToUse) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);
    scToUse.socket().setSoTimeout(MSocket.MSOCKET_HANDSHAKE_TIMEOUT);

    int ret = 0;
    while (ret < SetupControlMessage.SIZE)
    {
      MSocketLogger.getLogger().fine("setup control read happening");
      InputStream is = scToUse.socket().getInputStream();
      try
      {
        int currRead = is.read(buf.array(), ret, SetupControlMessage.SIZE - ret);
        MSocketLogger.getLogger().fine("read " + ret + " bytes");
        if (currRead == -1)
        {
          if (ret < SetupControlMessage.SIZE)
          {
            throw new java.net.ConnectException("Invalid handshake packet, probably not connected to an MServerSocket");
          }
        }
        else
          ret += currRead;
      }
      catch (SocketTimeoutException e)
      {
        throw new SocketTimeoutException("Timeout while establishing connection with MServerSocket.");
      }
    }
    MSocketLogger.getLogger().fine("setup control read complete");

    SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(buf.array());
    return scm;
  }
  
  /**
   * Synchronization of this method is callers responsibility
   * @param ControllerAddress
   * @param lfid
   * @param mstype
   * @param ControllerPort
   * @param SCToUse
   * @param SocketId
   * @param ProxyId
   * @param GUID
   * @param connectTime
   * @param cinfo
   * @throws IOException
   */
  public static void setupControlWrite
  		(InetAddress ControllerAddress, long lfid, int mstype, int ControllerPort,
	      SocketChannel SCToUse, int SocketId, int ProxyId, byte[] GUID, 
	      long connectTime, ConnectionInfo cinfo) 
	    		  throws IOException
  {
	  int DataAckSeq = 0;
	  if (cinfo != null)
	  {
		  {
			  DataAckSeq = cinfo.getDataAckSeq();
	      }
	  }
	  
	  if (mstype == SetupControlMessage.NEW_CON_MESG || 
			  mstype == SetupControlMessage.ADD_SOCKET)
	  {
		  DataAckSeq = (int) connectTime;
		  MSocketLogger.getLogger().fine("Connect time " + DataAckSeq);
	  }

	    SetupControlMessage scm = new SetupControlMessage(ControllerAddress, ControllerPort, lfid, DataAckSeq, mstype,
	        SocketId, ProxyId, GUID);
	    ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());

	    while (buf.remaining() > 0)
	    {
	      SCToUse.write(buf);
	    }

	    MSocketLogger.getLogger().fine("Sent IP:port " + ControllerPort + "; ackSeq = " + (cinfo != null ? DataAckSeq : 0));
	  }

  public static void main(String[] args)
  {
    try
    {
      // SetupControlMessage scm = new
      // SetupControlMessage(InetAddress.getLocalHost(), 2345, 473873211L,
      // 24567, 1, 1, 1);
      // byte[] enc = scm.getBytes();
      // SetupControlMessage dec =
      // SetupControlMessage.getSetupControlMessage(enc);
      // MSocketLogger.getLogger().fine(dec);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
}