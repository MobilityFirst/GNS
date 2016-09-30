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
public class InternalMSocket extends MSocket
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
  protected InternalMSocket(SocketChannel newChannel, MServerSocketController serverController,
      SetupControlMessage clientSCM) throws IOException
  {
    this.serverController = serverController;
    // necessary to set blocking mode to non-blocking
    // other wise due to race condition it blocks as by
    // default it is blocking mode
    setupControlServer(newChannel, clientSCM);
    if (cinfo == null)
      throw new IOException("Unexpected null cinfo");
    cinfo.setServerOrClient(MSocketConstants.SERVER);
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
   * Returns the flowID of the connection. The flowID is unique for a particular
   * server-client connection.
   * 
   * @return
   */
  public long getFlowID()
  {
    return flowID;
  }

  /**
   * @param scm
   */
  protected void setupServerController(SetupControlMessage scm)
  {
    MSocketLogger.getLogger().fine("Received IP:port " + scm.port + ":" + scm.iaddr + " for flowID " + flowID + "; ackSeq = " + scm.ackSeq);
    cinfo.setRemoteControlAddress(scm.iaddr);
    cinfo.setRemoteControlPort(scm.port);
  }

  // Server reads first, then writes
  // have to be synchronized, multiple threads calls it.
  // otherwise socketID might get same
  private synchronized void setupControlServer(SocketChannel newChannel, SetupControlMessage clientSCM)
      throws IOException
  {
    long localFlowID = (long) (Math.random() * Long.MAX_VALUE);

    SetupControlMessage scm = null;

    // FIXME: need some better way
    if (clientSCM == null)
    {

      scm = setupControlRead(newChannel);
    }
    else
    {
      scm = clientSCM;
    }

    cinfo = serverController.getConnectionInfo(scm.flowID);

    switch (scm.MesgType)
    {
      case SetupControlMessage.NEW_CON_MESG :
      case SetupControlMessage.NEW_CON_REQ :
      {
        MSocketLogger.getLogger().fine("NEW_CON_MESG recv at server from " + newChannel.socket().getInetAddress() + " scm.ackSeq "
            + scm.ackSeq);
        setupControlWrite(serverController.getLocalAddress(), localFlowID, SetupControlMessage.NEW_CON_MESG_REPLY,
            serverController.getLocalPort(), newChannel, scm.SocketId, scm.ProxyId, scm.GUID, 0);

        // new flowID is computed as average of both proposals for new
        // connections
        flowID = (localFlowID + scm.flowID) / 2;
        serverController.setConnectionInfo(this);
        MSocketLogger.getLogger().fine("Created new flow ID " + flowID);
        cinfo = serverController.getConnectionInfo(flowID);
        setupServerController(scm);

        // need to set it here
        newChannel.configureBlocking(false);
        SocketInfo sockInfo = new SocketInfo(newChannel, newChannel.socket(), scm.SocketId);

        // ack seq num overloads the rtt from
        // client to server in newe con message
        sockInfo.setEstimatedRTT(scm.ackSeq);

        cinfo.addSocketInfo(scm.SocketId, sockInfo);

        cinfo.inputQueuePutSocketInfo(sockInfo);
        cinfo.outputQueuePutSocketInfo(sockInfo);
        
        cinfo.setServerOrClient(MSocketConstants.SERVER);
        
        cinfo.setMSocketState(MSocketConstants.ACTIVE);
        serverController.getConnectionInfo(flowID).setState(ConnectionInfo.ALL_READY, true);
        MSocketLogger.getLogger().fine("Set server state to ALL_READY");

        //localTimer = new Timer();
        //startLocalTimer();
        //TimerTaskClass TaskObj = new TimerTaskClass(this);
        //(new Thread(TaskObj)).start();
        KeepAliveStaticThread.registerForKeepAlive(cinfo);

        break;
      }

      case SetupControlMessage.ADD_SOCKET :
      case SetupControlMessage.ADD_SOCKET_REQ :
      {
        MSocketLogger.getLogger().fine("ADD_SOCKET recv at server from " + newChannel.socket().getInetAddress() + " scm.ackSeq "
            + scm.ackSeq);

        setupControlWrite(serverController.getLocalAddress(), flowID, SetupControlMessage.ADD_SOCKET_REPLY,
            serverController.getLocalPort(), newChannel, scm.SocketId, scm.ProxyId, scm.GUID, 0);
        // need to set it here
        newChannel.configureBlocking(false);

        // ackseq num overloads the RTT from the client
        // to the server
        cinfo.addSocketHashMap(newChannel, scm.ackSeq);
        flowID = scm.flowID;
        isNew = false;
        break;
      }

      case SetupControlMessage.MIGRATE_SOCKET :
      case SetupControlMessage.MIGRATE_SOCKET_REQ :
      {
        cinfo = serverController.getConnectionInfo(scm.flowID);

        // server doesn't have a flowID that the client is trying to use to
        // migrate a previous connection. send reset.
        if (cinfo == null)
        {
          MSocketLogger.getLogger().fine("sending MIGRATE_SOCKET_RESET");
          setupControlWrite(serverController.getLocalAddress(), scm.flowID, SetupControlMessage.MIGRATE_SOCKET_RESET,
              serverController.getLocalPort(), newChannel, scm.SocketId, scm.ProxyId, scm.GUID, 0);
          isNew = false;
        }
        else
        {
          System.out.println("MIGRATE_SOCKET arrvied");
          setupControlWrite(serverController.getLocalAddress(), scm.flowID, SetupControlMessage.MIGRATE_SOCKET_REPLY,
              serverController.getLocalPort(), newChannel, scm.SocketId, scm.ProxyId, scm.GUID, 0);

          setupServerController(scm);

          /*
           * connection exists, so just change MSocket. Note that
           * setupControlServer (unlike setupControlClient) needs to change the
           * MSocket itself in addition to the underlying Socket as a new
           * MSocket is returned by the accept().
           */
          isNew = false;

          cinfo.closeAll(scm.SocketId);
          flowID = scm.flowID;

          cinfo.getObuffer().setDataBaseSeq(scm.ackSeq);

          SocketInfo sockObj = cinfo.getSocketInfo(scm.SocketId);

          while (!sockObj.acquireLock())
            ;

          sockObj.setneedToReqeustACK(true);
          // need to set it here
          newChannel.configureBlocking(false);
          sockObj.setSocketInfo(newChannel, newChannel.socket());
          sockObj.setStatus(true); // true means active
                                   // again

          sockObj.releaseLock();

          cinfo.inputQueuePutSocketInfo(sockObj);
          cinfo.outputQueuePutSocketInfo(sockObj);

          sockObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock()); //

          synchronized (cinfo.getSocketMonitor())
          {
            cinfo.getSocketMonitor().notifyAll(); // waking up blocked threads
          }

          ResendIfNeededThread RensendObj = new ResendIfNeededThread(cinfo);
          (new Thread(RensendObj)).start();
          
          System.out.println("MIGRATE_SOCKET complete");

        }

        break;
      }
    }
  }

  /**
   * Remove the current flow ID from the server controller map
   */
  protected void removeFlowId()
  {
    serverController.removeConnectionInfo(flowID);
  }
}