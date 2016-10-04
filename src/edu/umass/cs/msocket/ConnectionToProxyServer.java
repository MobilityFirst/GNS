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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the connection to the proxy. MServerSocket uses this
 * class to addProxy, removeProxy and accept connection from the proxy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ConnectionToProxyServer
{

  public static final int         GET                  = 1;                                                        // GET
                                                                                                                    // PUT
                                                                                                                    // operation
                                                                                                                    // in
                                                                                                                    // shared
                                                                                                                    // data
                                                                                                                    // structures
  public static final int         PUT                  = 2;
  public static final int         SIZE                 = 3;

  public final Object             addProxyMonitor      = new Object();
  public final Object             removeProxyMonitor   = new Object();
  public final Object             registerQueueMonitor = new Object();

  private MServerSocketController serverController     = null;
  private String                  serverGuid           = null;

  // queue to store the selector register objects
  private Queue<ProxyInfo>        registerQueue        = null;

  // uses selector for splicing
  private Selector                proxySelector        = null;

  public ConnectionToProxyServer(String guid, MServerSocketController serverController) throws IOException
  {
    this.serverGuid = guid;
    this.serverController = serverController;

    registerQueue = new LinkedList<ProxyInfo>();
    proxySelector = Selector.open();
  }

  public void closeProxyConnection()
  {
    try
    {
      proxySelector.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public String getServerName()
  {
    return serverController.getMServerSocket().getServerName();
  }

  public void addProxy(String Key, ProxyInfo Obj) throws IOException
  {
    synchronized (addProxyMonitor)
    {
      serverController.setProxyInfo(Key, Obj);
      // connect
      connect(Obj);

      // register with selector
      SocketChannel RegisteredChannel = Obj.getUnderlyingChannel();
      RegisteredChannel.configureBlocking(false);

      MSocketLogger.getLogger().fine("putiting new proxy in register queue");
      registerQueueOperations(PUT, Obj);
    }

  }

  public void removeProxy(String Key, ProxyInfo Obj) throws IOException
  {
    synchronized (removeProxyMonitor)
    { 
      serverController.removeProxyInfo(Key);

      // close the channel
      Obj.closeChannel();
      Obj.setActive(false);

      Obj.getSelectorKey().cancel(); 
    }
  }

  public MSocket accept() throws IOException
  {
    MSocketLogger.getLogger().fine("Proxy accept() called");
    MSocket ms = null;

    while (!serverController.isClosed())
    {
      // check for the queue, if there are any channels to register
      while (((Integer) registerQueueOperations(SIZE, null) != 0))
      {
        ProxyInfo regObj = (ProxyInfo) registerQueueOperations(GET, null);

        try
        {
          SelectionKey SelecKey = regObj.getUnderlyingChannel().register(proxySelector, SelectionKey.OP_READ, regObj);
          SelecKey.attach(regObj);
          regObj.setSelectorKey(SelecKey);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }

      int readyChannels = 0;
      try
      {
        readyChannels = proxySelector.select(); // changing it to select(),
                                                // makes it blocking, then it
                                                // deadlocks with register()
                                                // method on selector and
                                                // select() method here
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }

      if (readyChannels == 0)
        continue;

      Set<SelectionKey> selectedKeys = proxySelector.selectedKeys();

      Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

      while (keyIterator.hasNext())
      {
        SelectionKey key = keyIterator.next();
        if (key.isReadable())
        {
          // a channel is ready for reading
          ProxyInfo ProxyObj = (ProxyInfo) key.attachment();
          SocketChannel SourceChannel = ProxyObj.getUnderlyingChannel();
          try
          { 
          boolean exit = true;
          do
          {
            SetupControlMessage scm = null;

            ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);
            int numRead = 0;
            do
            {
              int curRead = SourceChannel.read(buf);
              numRead += curRead;
            }
            while ((numRead > 0) && (numRead < SetupControlMessage.SIZE));

            if (numRead == SetupControlMessage.SIZE)
            {
              // increment keep alive
              ProxyObj.setLastKeepAlive(serverController.getLocalClock());
              scm = SetupControlMessage.getSetupControlMessage(buf.array());
              if (scm.mesgType == SetupControlMessage.NEW_CON_REQ)
              {
                MSocketLogger.getLogger().fine("NEW_CON_REQ mesg recv from proxy");
                SocketChannel newChannel = SocketChannel.open();
                newChannel.connect(new InetSocketAddress(ProxyObj.getProxyName(), 
                		ProxyObj.getProxyPort()));
                
                while (!newChannel.finishConnect());
                
                MSocketLogger.getLogger().fine("Server's data channel port " 
                					+ newChannel.socket().getLocalPort());
                ms = new ServerMSocket(newChannel, serverController, scm);
                keyIterator.remove(); // imp bug: important to remove the
                                      // current key here, so that it can be
                                      // again returned by the selector,
                return ms;
              }
              else if (scm.mesgType == SetupControlMessage.MIGRATE_SOCKET_REQ)
              {
                MSocketLogger.getLogger().fine("MIGRATE_SOCKET_REQ mesg recv from proxy");
                SocketChannel newChannel = SocketChannel.open();
                
                newChannel.connect(new InetSocketAddress(ProxyObj.getProxyName(), 
                					ProxyObj.getProxyPort()));
                
                while (!newChannel.finishConnect());
                
                MSocketLogger.getLogger().fine("Server's data channel port " 
                					+ newChannel.socket().getLocalPort());
                ms = new ServerMSocket(newChannel, serverController, scm);
                exit = false;
              }
              else if (scm.mesgType == SetupControlMessage.ADD_SOCKET_REQ)
              {
                MSocketLogger.getLogger().fine("ADD_SOCKET_REQ mesg recv from proxy");
                SocketChannel newChannel = SocketChannel.open();
                newChannel.connect(new InetSocketAddress(ProxyObj.getProxyName(), 
                		ProxyObj.getProxyPort()));
                
                while (!newChannel.finishConnect());

                MSocketLogger.getLogger().fine("Server's data channel port " 
                							+ newChannel.socket().getLocalPort());
                ms = new ServerMSocket(newChannel, serverController, scm);
                exit = false;

              }
              else if (scm.mesgType == SetupControlMessage.KEEP_ALIVE)
              {
            	MSocketLogger.getLogger().fine("KEEP ALIVE mesg recv from proxy");
                //ProxyObj.setLastKeepAlive(serverController.getLocalClock());

                exit = false;
              }
            }
            else
            {
              exit = true;
            }
          }
          while (!exit);
        }
	      catch(Exception ex)
	      {
	    	  ex.printStackTrace();
	      }
	      
        }
        keyIterator.remove();
      }
    }
    
    return null;
  }

  private void setupControlWrite(long lfid, int mstype, int ControllerPort, int numOfSockets, SocketChannel SCToUse,
      int SocketId) throws IOException
  {
    byte[] GUID = new byte[SetupControlMessage.SIZE_OF_GUID];
    GUID = CommonMethods.hexStringToByteArray(serverGuid);
    
    MSocketLogger.getLogger().fine("serverGuid " + serverGuid + " GUID to be sent " + GUID + " length "
        + CommonMethods.hexStringToByteArray(serverGuid).length+" reverse "+CommonMethods.bytArrayToHex(GUID)+ " num of bytes "+ GUID.length);

    SetupControlMessage scm = new SetupControlMessage(SCToUse.socket().getLocalAddress(), ControllerPort, lfid, -1,
        mstype, SocketId, -1, GUID);
    ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());

    while (buf.remaining() > 0)
    {
      SCToUse.write(buf);
    }

  }

  private SetupControlMessage setupControlRead(SocketChannel SCToUse) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);

    while (buf.position() < SetupControlMessage.SIZE)
    {
      SCToUse.read(buf);
    }

    SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(buf.array());
    return scm;
  }

  private void connect(ProxyInfo proxyObj) throws IOException
  {
    proxyObj.connectChannel(proxyObj.getProxyName(), proxyObj.getProxyPort());
    SocketChannel proxyChannel = proxyObj.getUnderlyingChannel();
    Socket socket = proxyChannel.socket();

    MSocketLogger.getLogger().fine("Connected to proxy at " + socket.getInetAddress() + ":" + socket.getPort());

    setupControlWrite(-1, SetupControlMessage.CONTROL_SOCKET, -1, -1, proxyChannel, -1);

    // Read remote port, address, and flowID
    setupControlRead(proxyChannel);
  }

  private Object registerQueueOperations(int oper, ProxyInfo registerObj)
  {
    synchronized (registerQueueMonitor)
    {
      switch (oper)
      {
        case GET :
        {
          return registerQueue.poll();
        }
        case PUT :
        {
          registerQueue.add(registerObj);
          proxySelector.wakeup(); // wakeup method makes the blocking select
                                  // call to return
          // and check for the channels in the queue to register
          break;
        }
        case SIZE :
        {
          return registerQueue.size();
        }
      }
      return null;
    }
  }

}