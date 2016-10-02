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

package edu.umass.cs.msocket;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class defines an InternalMSocket used by MServerSocket for its
 * connections with MSocket clients.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ServerMSocket extends MSocket
{
  // Used only in case of server side Msocket, null for
  private MServerSocketController serverController = null;

  // is the msocket a new msocket or just created to facilitate migration
  private boolean                 isNew            = true;
  

  /**
   * Creates a new <code>InternalMSocket</code> object
   * 
   * @param newChannel
   * @param serverController
   * @param clientSCM
   * @throws IOException
   */
  public ServerMSocket(SocketChannel newChannel, 
		  MServerSocketController serverController,
      SetupControlMessage clientSCM) throws IOException
  {
    this.serverController = serverController;
    // necessary to set blocking mode to non-blocking
    // other wise due to race condition it blocks as by
    // default it is blocking mode
    setupControlServer(newChannel, clientSCM);
    if (connectionInfo == null)
      throw new IOException("Unexpected null cinfo");
    connectionInfo.setServerOrClient(MSocketConstants.SERVER);
  }

  /**
   * Returns whether this socket is new or a migrated socket. For the
   * applications, it will always return to be a new socket.
   * 
   * @return true: if the socket is new, false: if it is a migrated socket
   */
  public boolean isNew()
  {
    return isNew;
  }

  /**
   * @param scm
   */
  private void setupServerController(SetupControlMessage scm)
  {
    MSocketLogger.getLogger().fine
    ("Received IP:port " + scm.port + ":" + scm.iaddr + " for connID " 
    		+ connectionInfo.getConnID() + "; ackSeq = " + scm.ackSeq);
    connectionInfo.setRemoteControlAddress(scm.iaddr);
    connectionInfo.setRemoteControlPort(scm.port);
  }

  // Server reads first, then writes
  // have to be synchronized, multiple threads calls it.
  // otherwise socketID might get same
  private synchronized void setupControlServer(SocketChannel newChannel, 
		  SetupControlMessage clientSCM)
      throws IOException
  {
    long localConnID = (long) (Math.random() * Long.MAX_VALUE);

    SetupControlMessage scm = null;

    // FIXME: need some better way
    if (clientSCM == null)
    {
      scm = SetupControlMessage.setupControlRead(newChannel);
    }
    else
    {
      scm = clientSCM;
    }

    connectionInfo = serverController.getConnectionInfo(scm.connID);

    switch (scm.mesgType)
    {
      case SetupControlMessage.NEW_CON_MESG :
      case SetupControlMessage.NEW_CON_REQ :
      {
        MSocketLogger.getLogger().fine("NEW_CON_MESG recv at server from " 
        		+ newChannel.socket().getInetAddress() + " scm.ackSeq "
        		+ scm.ackSeq);
        SetupControlMessage.setupControlWrite(serverController.getLocalAddress(), 
        		localConnID, SetupControlMessage.NEW_CON_MESG_REPLY,
            serverController.getLocalPort(), newChannel, scm.socketID, 
            scm.proxyID, scm.GUID, 0, connectionInfo);

        // new flowID is computed as average of both proposals for new
        // connections
        long connID = (localConnID + scm.connID) / 2;
        serverController.setConnectionInfo(connID);
        MSocketLogger.getLogger().fine("Created new flow ID " + connID);
        connectionInfo = serverController.getConnectionInfo(connID);
        setupServerController(scm);

        // need to set it here
        newChannel.configureBlocking(false);
        SocketInfo sockInfo = new SocketInfo(newChannel, newChannel.socket(), scm.socketID);

        // ack seq num overloads the rtt from
        // client to server in newe con message
        sockInfo.setEstimatedRTT(scm.ackSeq);

        connectionInfo.addSocketInfo(scm.socketID, sockInfo);

        connectionInfo.inputQueuePutSocketInfo(sockInfo);
        connectionInfo.outputQueuePutSocketInfo(sockInfo);
        
        connectionInfo.setServerOrClient(MSocketConstants.SERVER);
        
        connectionInfo.setMSocketState(MSocketConstants.ACTIVE);
        serverController.getConnectionInfo(connID).setState(ConnectionInfo.ALL_READY, true);
        MSocketLogger.getLogger().fine("Set server state to ALL_READY");

        //localTimer = new Timer();
        //startLocalTimer();
        //TimerTaskClass TaskObj = new TimerTaskClass(this);
        //(new Thread(TaskObj)).start();
        KeepAliveStaticThread.registerForKeepAlive(connectionInfo);

        break;
      }

      case SetupControlMessage.ADD_SOCKET :
      case SetupControlMessage.ADD_SOCKET_REQ :
      {
        MSocketLogger.getLogger().fine("ADD_SOCKET recv at server from " + newChannel.socket().getInetAddress() + " scm.ackSeq "
            + scm.ackSeq);

        SetupControlMessage.setupControlWrite(serverController.getLocalAddress(), 
        		connectionInfo.getConnID(), 
        		SetupControlMessage.ADD_SOCKET_REPLY,
        		serverController.getLocalPort(), 
        		newChannel, scm.socketID, scm.proxyID, scm.GUID, 0, connectionInfo);
        // need to set it here
        newChannel.configureBlocking(false);

        // ackseq num overloads the RTT from the client
        // to the server
        connectionInfo.addSocketHashMap(newChannel, scm.ackSeq);
        //flowID = scm.flowID;
        isNew = false;
        break;
      }

      case SetupControlMessage.MIGRATE_SOCKET :
      case SetupControlMessage.MIGRATE_SOCKET_REQ :
      {
    	  connectionInfo = serverController.getConnectionInfo(scm.connID);

        // server doesn't have a flowID that the client is trying to use to
        // migrate a previous connection. send reset.
        if (connectionInfo == null)
        {
          MSocketLogger.getLogger().fine("sending MIGRATE_SOCKET_RESET");
          SetupControlMessage.setupControlWrite(serverController.getLocalAddress(), 
        		  scm.connID, SetupControlMessage.MIGRATE_SOCKET_RESET,
              serverController.getLocalPort(), newChannel, 
              scm.socketID, scm.proxyID, scm.GUID, 0, connectionInfo);
          isNew = false;
        }
        else
        {
          System.out.println("MIGRATE_SOCKET arrvied");
          SetupControlMessage.setupControlWrite(serverController.getLocalAddress(), 
        		  scm.connID, SetupControlMessage.MIGRATE_SOCKET_REPLY,
              serverController.getLocalPort(), newChannel, 
              scm.socketID, scm.proxyID, scm.GUID, 0, connectionInfo);

          setupServerController(scm);

          /*
           * connection exists, so just change MSocket. Note that
           * setupControlServer (unlike setupControlClient) needs to change the
           * MSocket itself in addition to the underlying Socket as a new
           * MSocket is returned by the accept().
           */
          isNew = false;

          connectionInfo.closeAll(scm.socketID);
          //flowID = scm.flowID;

          connectionInfo.getObuffer().setDataBaseSeq(scm.ackSeq);

          SocketInfo sockObj = connectionInfo.getSocketInfo(scm.socketID);

          while (!sockObj.acquireLock())
            ;

          sockObj.setneedToReqeustACK(true);
          // need to set it here
          newChannel.configureBlocking(false);
          sockObj.setSocketInfo(newChannel, newChannel.socket());
          sockObj.setStatus(true); // true means active
                                   // again

          sockObj.releaseLock();

          connectionInfo.inputQueuePutSocketInfo(sockObj);
          connectionInfo.outputQueuePutSocketInfo(sockObj);

          sockObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock()); //

          synchronized (connectionInfo.getSocketMonitor())
          {
        	  connectionInfo.getSocketMonitor().notifyAll(); // waking up blocked threads
          }

          ResendIfNeededThread RensendObj = new ResendIfNeededThread(connectionInfo);
          (new Thread(RensendObj)).start();
          
          System.out.println("MIGRATE_SOCKET complete");

        }

        break;
      }
    }
  }
  
}