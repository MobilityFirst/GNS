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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import edu.umass.cs.msocket.common.CommonMethods;
import edu.umass.cs.msocket.common.policies.BlackBoxWritingPolicy;
import edu.umass.cs.msocket.common.policies.ChunkInformation;
import edu.umass.cs.msocket.common.policies.MultipathWritingPolicy;
import edu.umass.cs.msocket.gns.Integration;
import edu.umass.cs.msocket.logger.MSocketLogger;
import edu.umass.cs.msocket.mobility.MobilityManagerClient;

/**
 * This class keeps the state associated for MSocket like socket maps,
 * connection state. This class implements many core functionalities in MSocket.
 * Each MSocket has an associated connectionInfo object.
 * 
 * @version 1.0
 */

public class ConnectionInfo
{
  // max unacked bytes, before which it sends ack
  private static final int         ACK_SEND_THRESH            = MWrappedOutputStream.WRITE_CHUNK_SIZE * 3;
  
  // num of close flowpaths message it needs to 
  // recieve before it closes the flowpaths
  private static final int 		   NUM_CLOSE_FPs			  = 1;
  
  
  // states of MSocket
  protected static final int       ALL_READY                  = 0;
  protected static final int       READ_WRITE                 = 1;
  protected static final int       CLOSED                     = 2;

  protected static final String[]  msgStr                     = {"ALL_READY", "READ_WRITE", "CLOSED"};

  protected static final int       BLOCKING_MULTIREAD         = 1;
  protected static final int       NONBLOCKING_MULTIREAD      = 2;

  /**
   * 5 seconds migration timeout
   */
  private static final int         MIGRATION_TIMEOUT          = 5000;

  /**
   * retransmission after 3 dup ack, as in TCP.
   */
  private static final int         MAX_DUP_ACK                = 5;

  /**
   * UDP controller attached to this MSocket
   */
  private InetAddress              controllerIP               = null;

  private final Object             socketMonitor              = new Object();
  private final Object             blockingFlagMonitor        = new Object();
  private final Object             stateMonitor               = new Object();

  private final Object             getActiveSocketMonitor     = new Object();

  private final Object             socketMapOperationsMonitor = new Object();

  private final Object             migrationMonitor           = new Object();
  private final Object             addSocketMonitor           = new Object();

  private final Object             migrateRemoteMonitor       = new Object();

  private final Object             inputStreamQueueMonitor    = new Object();
  private final Object             outputStreamQueueMonitor   = new Object();
  private final Object             inputStreamSelectorMonitor = new Object();

  private final Object             backgroundThreadMonitor    = new Object();
  
  private final Object			   emptyQueueThreadMonitor	  = new Object();

  //private MSocket                  msocket                    = null;
  private OutBuffer                obuffer                    = null;
  private InBufferOutOrder         ibuffer                    = null;
  private int                      remoteControlPort          = -1;
  private InetAddress              remoteControlAddress       = null;

  // Note: These sequence numbers are for control messages and are irrelevant
  // for data

  private int                      ctrlSendSeq                = 0;
  private int                      ctrlBaseSeq                = -1;
  private int                      ctrlAckSeq                 = 0;

  /**
   * sequence number of next byte to be sent
   */
  private int                      dataSendSeq                = 0;

  /**
   * sequence number of first byte yet to be received
   */
  private int                      dataAckSeq                 = 0;

  /**
   * beginning state
   */
  private int                      state                      = READ_WRITE;

  private boolean                  migrateRemote              = false;
  private boolean                  blockingFlag               = false;

  private MultipathPolicy          currentPolicy              = MultipathPolicy.MULTIPATH_POLICY_RANDOM;

  /**
   * for implementing uniform policy
   */
  public int                       interfaceNumToUse          = 0;

  // MSocket state different from READ_WRITE, ALL_READY state
  private int                      msocketState               = -1;

  /**
   * socketmap to store multipath sockets selector to which channels are
   * registered for blocking reads
   */
  private Map<Integer, SocketInfo> socketMap                  = null;
  private Selector                 inputStreamSelector        = null;

  // selector to which channels are registered for blocking writes
  private Selector                 outputStreamSelector       = null;

  // used to register channel for input stream selector
  private Queue<SocketInfo>        inputStreamQueue           = null;

  // used to register channel for output stream selector
  private Queue<SocketInfo>        outputStreamQueue          = null;

  /**
   * MSocket is at server or client side
   */
  private int                      serverOrClient             = -1;

  // variables moved from MSocket
  // stores what type ip, dnsname, gnsname, gns guid was given to connect
  private int                      typeOfCon                  = -1;

  // GUID associated with the alias for the server, and GUID of the server for
  // the client
  private String                   serverGUID                 = "";

  /**
   * ServerName required for GNRS query, store on initial connect. GUID alias or
   * DNS name
   */
  private String                   serverAlias                = "";

  /**
   * IP after resolving the alias
   */
  private InetAddress              serverIP                   = null;

  /**
   * Server port may not be required with GNS resolving
   */
  private int                      serverPort                 = -1;

  /**
   * 1 is always created within MSocket
   */
  private int                      nextSocketIdentifier       = 2;

  private boolean                  timerRunning               = true;

  /**
   * keeps track of num of dup ack recv
   */
  private int                      numDupAckRecv              = 0;

  // true means background writing thread running, false not
  private boolean                  backgroundThreadStatus     = false;
  
  //private BackgroundWritingThread  backWritingThread          = null;
  
  //empty queue thread
  //private BackgroundEmptyQueueThread  emptyQueueThread	      = null;

  //user set send buffer size
  private int                 	   userSetSendBufferSize	  = 0;
  
  
 
  private MultipathWritingPolicy multipathPolicy			  = null;
  
  private boolean backgroundThreadActive					  = false;
  
  private boolean emptyQueueActive					  		  = false;

  
  private final MServerSocketController serverController;
  private final long 					   connID;
  /**
   * Creates a new <code>ConnectionInfo</code> object
   * 
   * @param s
   */
  public ConnectionInfo(long connID , MServerSocketController serverController)
  {
	this.connID = connID;
	this.serverController = serverController;
    obuffer = new OutBuffer();
    ibuffer = new InBufferOutOrder();
    socketMap = new HashMap<Integer, SocketInfo>();

    try
    {
      inputStreamSelector = Selector.open();
      outputStreamSelector = Selector.open();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    inputStreamQueue = new LinkedList<SocketInfo>();
    outputStreamQueue = new LinkedList<SocketInfo>();
  }

  /**
   * starts retransmission thread.
   */
  /*public void startRetransmissionThread()
  {
    backWritingThread = new BackgroundWritingThread(this);
    new Thread(backWritingThread).start();
  }*/
  
  /**
   * starts empty Queue thread.
   */
  /*public void startEmptyQueueThread()
  {
    this.emptyQueueThread = new BackgroundEmptyQueueThread(this);
    new Thread(emptyQueueThread).start();
  }*/

  /**
   * return the socket state
   * 
   * @return
   */
  public int getMSocketState()
  {
    return this.msocketState;
  }
  
  

  public boolean getTimerStatus()
  {
    return timerRunning;
  }

  /**
   * will be moved to ConnectionInfo
   * 
   * @param state
   */
  public void setMSocketState(int state)
  {
    this.msocketState = state;
  }

  /**
   * returns whether this MSocket is from the server or client side,
   * 
   * @return
   */
  public int getServerOrClient()
  {
    return serverOrClient;
  }

  public void setServerOrClient(int serverOrClient)
  {
    this.serverOrClient = serverOrClient;
  }

  public Selector getInputStreamSelector()
  {
    return inputStreamSelector;
  }

  public Selector getOutputStreamSelector()
  {
    return outputStreamSelector;
  }

  public SocketInfo inputQueueGetSocketInfo()
  {
    synchronized (inputStreamQueueMonitor)
    {
      return inputStreamQueue.poll();
    }
  }

  public int inputQueueGetSize()
  {
    synchronized (inputStreamQueueMonitor)
    {
      return inputStreamQueue.size();
    }

  }

  public void inputQueuePutSocketInfo(SocketInfo sockInfo)
  {
    synchronized (inputStreamQueueMonitor)
    {
      inputStreamQueue.add(sockInfo);
      inputStreamSelector.wakeup();
      // wakeup method makes the blocking
      // select call to return
      // and check for the channels in the queue to register
    }
  }

  public SocketInfo outputQueueGetSocketInfo()
  {
    synchronized (outputStreamQueueMonitor)
    {
      return outputStreamQueue.poll();
    }
  }

  public int outputQueueGetSize()
  {
    synchronized (outputStreamQueueMonitor)
    {
      return outputStreamQueue.size();
    }
  }

  public void outputQueuePutSocketInfo(SocketInfo sockInfo)
  {
    synchronized (outputStreamQueueMonitor)
    {
      outputStreamQueue.add(sockInfo);
      outputStreamSelector.wakeup();
      // wakeup method makes the blocking
      // select call to return
      // and check for the channels in the queue to register
    }
  }

  public void setMultipathPolicy(MultipathPolicy policy)
  {
    currentPolicy = policy;
  }

  public MultipathPolicy getMultipathPolicy()
  {
    return currentPolicy;
  }

  /*
   * flowID is set just once in the beginning by MSocket, so no synchronization
   * is needed.
   */
  public long getConnID()
  {
    return this.connID;
  }

  public void setblockingFlag(boolean value)
  {
    synchronized (getBlockingFlagMonitor())
    {
      blockingFlag = value;
      if (value == false)
      {
        getBlockingFlagMonitor().notifyAll();
      }
    }
  }

  public boolean getblockingFlag()
  {
    return blockingFlag;
  }

  /*
   * The methods below are invoked by just one thread, the MSocket thread, so no
   * synchronization is needed.
   */
  public void setRemoteControlPort(int p)
  {
    remoteControlPort = p;
  }

  public void setRemoteControlAddress(InetAddress iaddr)
  {
    remoteControlAddress = iaddr;
  }

  public int getRemoteControlPort()
  {
    return remoteControlPort;
  }

  public InetAddress getRemoteControlAddress()
  {
    return remoteControlAddress;
  }

  public int getCtrlSendSeq()
  {
    return ctrlSendSeq;
  }

  public int getCtrlBaseSeq()
  {
    return ctrlBaseSeq;
  }

  public int getCtrlAckSeq()
  {
    return ctrlAckSeq;
  }

  public void setCtrlSendSeq(int s)
  {
    ctrlSendSeq = s;
  }

  public void setCtrlBaseSeq(int s)
  {
    ctrlBaseSeq = s;
  }

  public void setCtrlAckSeq(int s)
  {
    ctrlAckSeq = s;
  }

  public int getDataAckSeq()
  {
    return dataAckSeq;
  }

  public int getDataSendSeq()
  {
    return dataSendSeq;
  }

  public byte[] getDataFromOutBuffer(long startSeqNum, long EndSeqNum)
  {
    return getObuffer().getDataFromOutBuffer(startSeqNum, EndSeqNum);
  }

  public synchronized void updateDataSendSeq(int s)
  {
    dataSendSeq += s;
  }

  public synchronized void updateDataAckSeq(int s)
  {
    dataAckSeq += s;
  }

  public boolean notAckedInAWhile(SocketInfo Obj)
  {
    if ((Obj.getRecvdBytes() - Obj.getLastNumBytesRecv()) >= ACK_SEND_THRESH)
      return true;
    else
      return false;
  }

  public long getDataBaseSeq()
  {
    // obuffer.setDataBaseSeq(bs);
    return getObuffer().getDataBaseSeq();
  }

  // OutBuffer internally synchronized, so no synchronization needed

  public byte[] getUnacked(int bs)
  {
    getObuffer().setDataBaseSeq(bs);
    return getObuffer().getUnacked();
  }

  public byte[] getUnacked()
  {
    return getObuffer().getUnacked();
  }

  // Only called and read by Controller, so no synchronization needed

  public void setMigrateRemote(boolean b)
  {
    migrateRemote = b;
  }

  public boolean getMigrateRemote()
  {
    return migrateRemote;
  }

  // OutBuffer is internally synchronized, so no synchronization needed
  public boolean addOutBuffer(byte[] buf, int offset, int length)
  {
    // TODO: modify this part to restrict outbuffer based on system's heap size
    return getObuffer().add(buf, offset, length);
  }

  public void setCloseInOutbuffer(boolean value)
  {
    getObuffer().Close_Obuffer = value;
  }

  public void setACKInOutbuffer(boolean value)
  {
    getObuffer().ACK_Obuffer = value;
  }

  public boolean getCloseInOutbuffer()
  {
    return getObuffer().Close_Obuffer;
  }

  public boolean getACKInOutbuffer()
  {
    return getObuffer().ACK_Obuffer;
  }

  public int getOutBufferSize()
  {
    return getObuffer().getOutbufferSize();
  }

  public void releaseOutBuffer()
  {
    getObuffer().releaseOutBuffer();
  }

  public void ackOutBuffer(long ack)
  {
    getObuffer().ack(ack);
  }

  public boolean addInBuffer(InBufferStorageChunk Obj)
  {

    return ibuffer.putInBuffer(Obj);
  }

  public int readInBuffer(byte[] b, int offset, int length)
  {
    return ibuffer.getInBuffer(b, offset, length);
  }

  public long getInBufferSize()
  {
    return ibuffer.getInBufferSize();
  }

  public int getState()
  {
    return state;
  }

  public synchronized boolean getBackgroundThreadStatus()
  {
    return this.backgroundThreadStatus;
  }

  public synchronized void setBackgroundThreadStatus(boolean status)
  {
    this.backgroundThreadStatus = status;
  }

  public boolean setState(int s, boolean blocking)
  {
    synchronized (stateMonitor)
    {
      boolean ret = false;
      if (s == CLOSED)
      {
        state = s;
        notifyAll();
        return true;
      }
      switch (state)
      {
        case ALL_READY :
        {
          state = s;
          ret = true;
          break;
        }
        case READ_WRITE :
        {
          if (s == ALL_READY) // sync problems everywhere, can't allow to go
                              // from READ_WRITE to MIGRATE as don't want to
                              // write to socket data and control message
                              // simulatanoeusly
          {
            state = s;
            ret = true;
          }
          break;
        }
      }

      if (s == ALL_READY)
      {
        stateMonitor.notifyAll();
      }

      if (blocking)
      {
        if (!ret)
        {
          MSocketLogger.getLogger().fine("Failed to change state to " + msgStr[s] + " from " + msgStr[state]);

          while (state != ALL_READY)
          {
            try
            {
              stateMonitor.wait();
            }
            catch (InterruptedException e)
            {
              e.printStackTrace();
            }
          }
          state = s;
          ret = true;
        }
      }
      return ret;
    }
  }
  
//  public MSocket getMSocket()
//  {
//    return msocket;
//  }
  
  
  public void setBackgroundThreadActive(boolean status)
  {
	  synchronized(this.backgroundThreadMonitor)
	  {
		  this.backgroundThreadActive = status;
	  }
  }
  
  public boolean getBackgroundThreadActive()
  {
	  synchronized(this.backgroundThreadMonitor)
	  {
		  return this.backgroundThreadActive;
	  }
  }
  
  public void setEmptyQueueActive(boolean status)
  {
	  synchronized(this.emptyQueueThreadMonitor)
	  {
		  this.emptyQueueActive = status;
	  }
  }
  
  public boolean getEmptyQueueActive()
  {
	  synchronized(this.emptyQueueThreadMonitor)
	  {
		  return this.emptyQueueActive;
	  }
  }

  /**
   * Return the socket info given a socket identifier
   * 
   * @param socketIdetifier
   * @return
   */
  public SocketInfo getSocketInfo(int socketIdetifier)
  {
    synchronized (socketMapOperationsMonitor)
    {
      return socketMap.get(socketIdetifier);
    }
  }
  
  public void setMultipathWritingPolicy(MultipathWritingPolicy writingPolicy)
  {
	  this.multipathPolicy = writingPolicy;
  }
  
  public MultipathWritingPolicy getMultipathWritingPolicy()
  {
	  return this.multipathPolicy;
  }

  /**
   * Add the given socket info associated to the socket identifier
   * 
   * @param socketIdetifier
   * @param sockInfo
   */
  public void addSocketInfo(int socketIdetifier, SocketInfo sockInfo)
  {
    synchronized (socketMapOperationsMonitor)
    {
      socketMap.put(socketIdetifier, sockInfo);
      // notify all

      synchronized (getSocketMonitor())
      {
        getSocketMonitor().notifyAll(); // waking up blocked threads
      }
    }
  }

  /**
   * Remove the socket info given a socket identifier
   * 
   * @param socketIdetifier
   * @return the previous SocketInfo associated with the socket identifier, or
   *         null if there was no mapping for this socket id
   */
  public SocketInfo removeSocketInfo(int socketIdetifier)
  {
    synchronized (socketMapOperationsMonitor)
    {
      return socketMap.remove(socketIdetifier);
    }
  }

  /**
   * Return a collection of SocketInfo of all registered flowpaths.
   * 
   * @return
   */
  public Collection<SocketInfo> getAllSocketInfo()
  {
    synchronized (socketMapOperationsMonitor)
    {
      return socketMap.values();
    }
  }
  
  public SocketInfo getActiveSocket(MultipathPolicy writePolicy)
  {
    // synchronization reqd mainly for the default policy
    synchronized (getActiveSocketMonitor)
    {
      SocketInfo Obj = null;
      Vector<SocketInfo> socketMapValues = new Vector<SocketInfo>();
      socketMapValues.addAll(getAllSocketInfo());

      switch (writePolicy)
      {
        case MULTIPATH_POLICY_RANDOM:
        {
          Random generator = new Random();
          Vector<SocketInfo> vect = new Vector<SocketInfo>();

          int i = 0;
          while (i < socketMapValues.size())
          {
            SocketInfo value = socketMapValues.get(i);

            if (value.getStatus()) // true means active
            {
              vect.add(value);
            }
            i++;
          }
          if (vect.size() == 0) // denotes all sockets under migration
            return null;

          int index = generator.nextInt(vect.size());
          Obj = vect.get(index); // randomly choosing the socket to send chunk
          break;
        }

        case MULTIPATH_POLICY_OUTSTAND_RATIO:
        {
          int i = 0;
          double minRatio = -1;

          while (i < socketMapValues.size())
          {
            SocketInfo value = socketMapValues.get(i);

            if (value.getStatus()) // true means active
            {
              MSocketLogger.getLogger().fine("Socket ID " + value.getSocketIdentifer() + " outstanding bytes "
                  + value.getOutStandingBytesRatio());
              if ((minRatio == -1) || (value.getOutStandingBytesRatio() < minRatio))
              {
                minRatio = value.getOutStandingBytesRatio();
                Obj = value;
              }
            }
            i++;
          }
          break;
        }

        case MULTIPATH_POLICY_UNIFORM :
        {
          int i = 0;
          SocketInfo value = null;
          while (i < socketMapValues.size())
          {
            value = socketMapValues.get(i);
            if (value.getStatus()) // true means active
            {
              if (i >= interfaceNumToUse) // return the running interface after
                                          // the desired interface to use
              {
                break;
              }
            }
            i++;
          }
          int Size = socketMapValues.size();
          interfaceNumToUse++;
          interfaceNumToUse = interfaceNumToUse % Size;
          return value;
        }
        default:
        {
        	try 
        	{
				throw new Exception("Multipath policy not supported");
			} catch (Exception e) 
        	{
				e.printStackTrace();
			}
        }
      }
      return Obj;
    }
  }

  /**
   * @param dataChannel
   * @return
   * @throws IOException
   */
  public DataMessage readDataMessageHeader(SocketChannel dataChannel) throws IOException
  {
    int nreadHeader = 0;
    ByteBuffer buf = ByteBuffer.allocate(DataMessage.sizeofHeader());

    DataMessage dm = null;

    do
    {
      int cur = 0;
      cur = dataChannel.read(buf);
      if (cur != -1)
        nreadHeader += cur;
      else
      {
        break;
      }
    }
    while ((nreadHeader > 0) && (nreadHeader != DataMessage.sizeofHeader()));
    if (nreadHeader == DataMessage.sizeofHeader())
    {
      buf.flip();
      dm = DataMessage.getDataMessageHeader(buf.array());
    }
    return dm;
  }

  /**
   * @param flowID
   * @param dataChannel
   * @throws IOException
   */
  public void sendDataAckOnly(long flowID, SocketInfo Obj, int ackForSeqNum) 
  {
    if (Obj == null) // means no active channels, return;
      return;

    MSocketLogger.getLogger().fine("sendDataAckOnly entered socket ID " + Obj.getSocketIdentifer());

    if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
    {
      return;
    }

    if (!notAckedInAWhile(Obj)) // not flooding ACKs to sender
      return;

    try
    {
      SocketChannel dataChannel = Obj.getDataChannel();

      int DataAckSeq = getDataAckSeq();

      DataMessage dm = new DataMessage(DataMessage.DATA_ACK_REP, getDataSendSeq(), DataAckSeq, ackForSeqNum,
          Obj.getRecvdBytes(), null, -1);

      byte[] buf = dm.getBytes();

      ByteBuffer bytebuf = null;
      bytebuf = ByteBuffer.wrap(buf);

      boolean firstWrite = true;
      // tries to write, if it writes some bytes first time, then it writes full
      // otherwise doesn't write ack, as we don't want to block reads for
      // writing acks
      while (bytebuf.hasRemaining())
      {
        int numWritten = dataChannel.write(bytebuf);
        if ((numWritten == 0) && (firstWrite))
        {
          firstWrite = false;
          break;
        }
        firstWrite = false;
      }

      // only update if ack was sent successfully
      if (!bytebuf.hasRemaining())
      {
        Obj.setLastNumBytesRecv();
      }
      MSocketLogger.getLogger().fine("DATA ACK sent DataAckSeq " + DataAckSeq + " Obj.getRecvdBytes() " + Obj.getRecvdBytes()
          + " socket ID " + Obj.getSocketIdentifer());
    }
    catch (IOException ex)
    {
      MSocketLogger.getLogger().fine("IO exception while sending ACK");
    }
  }

  /**
   * reads from multiple sockets, each message should be completely till whole
   * length mentioned in data header, partially read messages may get discarded.
   * 
   * @throws IOException
   */
  public int multiSocketRead() throws IOException
  {
    if (getMSocketState() == MSocketConstants.CLOSED)
    {
      throw new IOException(" socket already closed");
    }

    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(socketMap.values());
    int i = 0;
    int totalread = 0;

    while (i < vect.size())
    {
      SocketInfo value = vect.get(i);
      
      /*if(getMSocketState() != MSocketConstants.ACTIVE)
      {
    	  System.out.println("reading flowpath id "+value.getSocketIdentifer() );
      }*/
      
      try
      {
        if (value.getStatus()) // only read active sockets
        {
          int ret = 0;
          boolean acksend = false;
          do
          {
            ret = singleSocketRead(value);
            if (ret > 0)
            {
              totalread += ret;
              acksend = true;
              value.updateRecvdBytes(ret);
            }
            if (getMSocketState() == MSocketConstants.CLOSED)
            {
              break;
            }
          }
          while ((ret == -2)); // read if a header was last read

          if (getMSocketState() == MSocketConstants.CLOSED)
          {
            break;
          }

          //if (acksend)
          //  sendDataAckOnly(getFlowID(), value);
        }
      }
      catch (IOException ex)
      {
        MSocketLogger.getLogger().fine("read exception caused IOException for socket with Id " + value.getSocketIdentifer());
        while (!value.acquireLock())
          ;
        value.setStatus(false);
        value.setneedToReqeustACK(true);
        value.releaseLock();

        while (value.getneedToReqeustACK())
        {
          SocketInfo Obj = getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM);
          if (Obj != null)
          {
            while (!Obj.acquireLock())
              ;
            try
            {
              handleMigrationInMultiPath(Obj);
              value.setneedToReqeustACK(false);
            }
            catch (IOException e)
            {
              e.printStackTrace();
              MSocketLogger.getLogger().fine("HandleMigrationInMultiPath  read exception caused IOException for socket with Id "
                  + value.getSocketIdentifer());

              Obj.setStatus(false);
              Obj.setneedToReqeustACK(true);
            }
            Obj.releaseLock();
          }
          else
          {
            synchronized (getSocketMonitor())
            {
              while ((getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM) == null)
                  && (getMSocketState() == MSocketConstants.ACTIVE))
              {
                try
                {
                  getSocketMonitor().wait();
                }
                catch (InterruptedException e)
                {
                  e.printStackTrace();
                }
              }

              if (getMSocketState() == MSocketConstants.CLOSED)
              {
                throw new IOException(" socket already closed");
              }
            }
          }
        }
      }
      i++;
      
      if (getMSocketState() == MSocketConstants.CLOSED)
      {
        MSocketLogger.getLogger().fine("close message recvd");
        break;
      }
    }
    return totalread;
  }

  /**
   * re arranges the socket vector to read from the one that has stream seq num
   * closer to datareadseq num. to give preference to inordered reads
   * 
   * @return
   */
  public Vector<SocketInfo> rearrangeSocketVector(Vector<SocketInfo> socketVect)
  {
    Vector<SocketInfo> ordered = new Vector<SocketInfo>();

    for (int i = 0; i < socketVect.size(); i++)
    {
      if (socketVect.get(i).getStatus())
      {
        int insertIndex = 0;
        for (insertIndex = 0; insertIndex < ordered.size(); insertIndex++)
        {
          if (socketVect.get(i).getChunkReadOffsetSeqNum() < ordered.get(insertIndex).getChunkReadOffsetSeqNum())
          {
            break;
          }
        }
        ordered.add(insertIndex, socketVect.get(i));
      }
    }

    return ordered;
  }

  /**
   * reads from multiple sockets, each message should be completely till whole
   * length mentioned in data header, partially read messages may get discarded.
   * 
   * @throws IOException
   */
  public int multiSocketRead(byte[] b, int offset, int length) throws IOException
  {
    if (getMSocketState() == MSocketConstants.CLOSED)
    {
      throw new IOException(" socket already closed");
    }

    if(this.getServerOrClient() == MSocketConstants.SERVER)
    {
    	MSocketLogger.getLogger().fine("multiSocketRead happening");
    }
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    // Vector<SocketInfo> ordered = rearrangeSocketVector(vect);
    Vector<SocketInfo> ordered = vect;

    int i = 0;
    int readInAppBuffer = 0;

    boolean dataReadAppBuffer = false;

    while (i < ordered.size())
    {
      SocketInfo value = ordered.get(i);

      try
      {
        if (value.getStatus()) // only read active sockets
        {
          MSocketInstrumenter
              .updateRecvBufferSize(value.getSocket().getReceiveBufferSize(), value.getSocketIdentifer());
          if (!dataReadAppBuffer)
          {
            SingleSocketReadReturnInfo retObject = null;
            boolean acksend = false;
            do
            {
              long ssrStart = System.currentTimeMillis();
              retObject = singleSocketRead(value, b, offset, length);
              long ssrEnd = System.currentTimeMillis();
              MSocketInstrumenter.addSingleSocketReadSample((ssrEnd - ssrStart));

              if (retObject.numBytesRead > 0)
              {
            	  if(this.getServerOrClient() == MSocketConstants.SERVER)
            		  MSocketLogger.getLogger().fine("data read from socket id " + value.getSocketIdentifer() + " read " + retObject.numBytesRead);
                
            	MSocketInstrumenter.updateSocketReads(retObject.numBytesRead, value.getSocketIdentifer());
                acksend = true;
                value.updateRecvdBytes(retObject.numBytesRead);
              }
            }
            while (retObject.typeOfRead == SingleSocketReadReturnInfo.DATAMESSAGEHEADER); // read
                                                                                          // if
                                                                                          // a
                                                                                          // header
                                                                                          // was
                                                                                          // last
                                                                                          // read
                                                                                          // or
                                                                                          // some
                                                                                          // data,
            // so that there may be more data.

            /*if (acksend)
            {
              long sdaStart = System.currentTimeMillis();
              sendDataAckOnly(getFlowID(), value);
              long sdaEnd = System.currentTimeMillis();
              MSocketInstrumenter.addDataAckSendSample((sdaEnd - sdaStart));
            }*/
            if ((retObject.typeOfRead == SingleSocketReadReturnInfo.COPIEDAPPBUFFER) && (retObject.numBytesRead > 0))
            {
              readInAppBuffer = retObject.numBytesRead;
              dataReadAppBuffer = true;
              // read done break;
              // break;
            }
          } // if it has read into app buffer, then it reads form other socket
            // and copies data in
          // in input buffer
          else
          {
        	if(this.getServerOrClient() == MSocketConstants.SERVER)
        		MSocketLogger.getLogger().fine("multisocket read in else case.");
        	
            int ret = 0;
            boolean acksend = false;
            do
            {
              long ssrStart = System.currentTimeMillis();
              ret = singleSocketRead(value);
              long ssrEnd = System.currentTimeMillis();
              MSocketInstrumenter.addSingleSocketReadSample((ssrEnd - ssrStart));

              if (ret > 0)
              {
                acksend = true;
                value.updateRecvdBytes(ret);
                MSocketInstrumenter.updateSocketReads(ret, value.getSocketIdentifer());
              }
            }
            while (ret == -2); // read if a header was last read
                               // or
                               // some data, so that there may be
                               // more data.

            /*if (acksend)
            {
              long sdaStart = System.currentTimeMillis();
              sendDataAckOnly(getFlowID(), value);
              long sdaEnd = System.currentTimeMillis();
              MSocketInstrumenter.addDataAckSendSample((sdaEnd - sdaStart));
            }*/
          }

        }
      }
      catch (IOException ex)
      {
        MSocketLogger.getLogger().fine("read exception caused IOException for socket with Id " + value.getSocketIdentifer());
        while (!value.acquireLock())
          ;
        value.setStatus(false);
        value.setneedToReqeustACK(true);
        value.releaseLock();

        while (value.getneedToReqeustACK())
        {
          SocketInfo Obj = getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM);
          if (Obj != null)
          {
            while (!Obj.acquireLock())
              ;
            try
            {
              handleMigrationInMultiPath(Obj);
              value.setneedToReqeustACK(false);
            }
            catch (IOException e)
            {
              e.printStackTrace();
              MSocketLogger.getLogger().fine("HandleMigrationInMultiPath  read exception caused IOException " + "for socket with Id "
                  + value.getSocketIdentifer());

              Obj.setStatus(false);
              Obj.setneedToReqeustACK(true);
            }
            Obj.releaseLock();
          }
          else
          {
            synchronized (getSocketMonitor())
            {
              while ((getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM) == null)
                  && (getMSocketState() == MSocketConstants.ACTIVE))
              {
                try
                {
                  getSocketMonitor().wait();
                }
                catch (InterruptedException e)
                {
                  e.printStackTrace();
                }
              }
            }

            if (getMSocketState() == MSocketConstants.CLOSED)
            {
              throw new IOException(" socket already closed");
            }

          }
        }
      }
      i++;
    }
    
    if(this.getServerOrClient() == MSocketConstants.SERVER)
    {
    	MSocketLogger.getLogger().fine("multiSocketRead complete "+ readInAppBuffer);
    }
    return readInAppBuffer;
  }

  public int multiSocketKeepAliveRead()
  {
    // Not in active read or write state, must be in closing state
    // or closed state. In that case, all keep alve reads happen
    // in close method in MSocket.
    if (this.getMSocketState() != MSocketConstants.ACTIVE)
    {
      return 0;
    }

    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    int i = 0;
    int totalread = 0;

    while (i < vect.size())
    {
      SocketInfo value = vect.get(i);
      try
      {
        if (value.getStatus()) // only read active sockets
        {
          int ret = 0;
          // do
          // {
          ret = singleSocketRead(value);
          if (ret > 0)
          {
            totalread += ret;
            value.updateRecvdBytes(ret);
          }
          // }
          // while (ret == -2); // read just once here, we don't want
          // the state to change from ACTIVE to
          // some state in Close state machine and
          // and closed state messages get lost
          checkToStartDataAckThread(value);
        }
      }
      catch (IOException ex)
      {
        MSocketLogger.getLogger().fine("read exception caused IOException for socket with Id " + value.getSocketIdentifer());
        while (!value.acquireLock());
        value.setStatus(false);
        value.setneedToReqeustACK(true);
        value.releaseLock();
      }
      i++;
    }
    return totalread;
  }

  public void closeAll() throws IOException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());

    for (SocketInfo value : vect)
    {
      if (value.getStatus())
      {
        // flush and close
        value.getDataChannel().close();
        value.getSocket().close();
      }
    }
  }
  
  public void sendKeepAliveOnAllPaths()
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    int i = 0;

    while (i < vect.size())
    {
      SocketInfo value = vect.get(i);

      try
      {
        if (value.getStatus()) // only active sockets
        {
        	// both the other side and it has received req num of close FP, close the socket
        	if( (value.getNumFPRecvdOtherSide() >= ConnectionInfo.NUM_CLOSE_FPs) 
        			&& (value.getNumFPRecvd() >= ConnectionInfo.NUM_CLOSE_FPs) )
        	{
        		System.out.println("Satisfied the closing condition of a flowpath, " +
        				" closing it finally flowpath id "+value.getSocketIdentifer() +" msocket type "+this.getServerOrClient());
        		SocketInfo socketObj = removeSocketInfo(value.getSocketIdentifer());
        	    while (!socketObj.acquireLock());
        	    socketObj.setStatus(false);
        	    socketObj.releaseLock();
        	    i++;
        	    continue;
        	}
        	
        	if ( (Integer) value.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0 )
            {
        		i++;
        		continue;
            }
        	
        	
        	int mesgType;
        	if( value.getClosing() )
        	{
        		System.out.println("Sending CLOSE_FP");
        		mesgType = DataMessage.CLOSE_FP;
        	}
        	else
        	{
        		mesgType = DataMessage.KEEP_ALIVE;
        	}
        	
        	DataMessage dm = new DataMessage(mesgType, getDataSendSeq(), getDataAckSeq(), 0, value.getNumFPRecvd(), null, -1);
          
        	byte[] buf = dm.getBytes();

        	ByteBuffer bytebuf = null;
        	bytebuf = ByteBuffer.wrap(buf);
        	while (bytebuf.hasRemaining())
        		value.getDataChannel().write(bytebuf);
        }
      }
      catch (IOException ex)
      {
        MSocketLogger.getLogger().fine("IOException for socket with Id " + value.getSocketIdentifer());
        ex.printStackTrace();
      }
      i++;
    }
  }

  /**
   * Checks the 3 dup ack and if true
   * 
   * @return
   */
  public boolean checkDuplicateAckCondition()
  {
    if (numDupAckRecv > MAX_DUP_ACK)
    {
      numDupAckRecv = 0;
      return true;
    }
    else
    {
      return false;
    }
  }

  public void resetDupAckCounter()
  {
    numDupAckRecv = 0;
  }

  private void handleMigrationInMultiPath(SocketInfo Obj) throws IOException
  {
    // if queue size is > 0 then it means that there is a non-blocking
    // write pending and it should be sent first, instead of migration data
    if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
    {
      //attemptSocketWrite(Obj);
      return;
    }

    int dataSendSeqNum = getDataSendSeq();
    MSocketLogger.getLogger().fine("handleMigrationInMultiPath End Seq Num" + dataSendSeqNum + " SocektId " + Obj.getSocketIdentifer());
    int DataAck = (int) getDataBaseSeq();
    MSocketLogger.getLogger().fine("DataAck from other side " + DataAck);

    if (dataSendSeqNum > DataAck)
    {
      byte[] buf = getDataFromOutBuffer(DataAck, dataSendSeqNum);

      int arrayCopyOffset = 0;
      DataMessage dm = new DataMessage(DataMessage.DATA_MESG, DataAck, getDataAckSeq(), buf.length, 0, buf,
          arrayCopyOffset);
      byte[] writebuf = dm.getBytes();
      ByteBuffer bytebuf = ByteBuffer.allocate(writebuf.length);
      bytebuf.put(writebuf);
      bytebuf.flip();
      while (bytebuf.hasRemaining())
        Obj.getDataChannel().write(bytebuf);
    }
    Obj.setneedToReqeustACK(false);
  }

  /**
   * @param flowID
   * @throws IOException
   */
  private void sendCloseAckOnly(long flowID) throws IOException
  {
    // empty the write queues before
    // writing anything, so not to
    // desynchronize the outputstream
    emptyTheWriteQueues();
    int DataAckSeq = getDataAckSeq();

    DataMessage dm = new DataMessage(DataMessage.ACK, getDataSendSeq(), DataAckSeq, 0, 0, null, -1);
    byte[] buf = dm.getBytes();
    ByteBuffer bytebuf = null;
    bytebuf = ByteBuffer.wrap(buf);
    SocketInfo socketInfo = getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM);
    MSocketLogger.getLogger().fine("sendCloseAckOnly on " + socketInfo.getSocketIdentifer());
    while (bytebuf.hasRemaining())
      socketInfo.getDataChannel().write(bytebuf);

    socketInfo.setLastNumBytesRecv();
  }

  private int singleSocketRead(SocketInfo socketObj) throws IOException
  {
    long assrStart = System.currentTimeMillis();
    SocketChannel dataChannel = socketObj.getDataChannel();
    dataChannel.configureBlocking(false);

    int nread = 0;
    boolean EOF = false;
    int ndirect = socketObj.canReadDirect();

    ByteBuffer buf = ByteBuffer.allocate(ndirect);
    
    if (ndirect > 0)
    {
      int cur = 0;
      cur = dataChannel.read(buf);

      if (cur > 0)
      { // needed because resendIf needed may prevet keep alive sending from
        // server for more than 10 secs, threshold for proxy failure
        if (getServerOrClient() == MSocketConstants.CLIENT)
        {
        	socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
        }
      }

      if (cur != -1)
        nread += cur;
      else
        EOF = true;
    }
    else
    { // ndirect==0

      long dmhStart = System.currentTimeMillis();
      DataMessage dmheader = readDataMessageHeader(dataChannel);
      long dmhEnd = System.currentTimeMillis();

      MSocketInstrumenter.addDataMessageHeaderSample(dmhEnd - dmhStart);

      if (dmheader != null)
      {
        socketObj.setChunkReadOffsetSeqNum(dmheader.sendSeq);
        
        // in DATA_ACK_REP, length field is selective ACK
        if(dmheader.Type != DataMessage.DATA_ACK_REP)
        {
        	socketObj.setchunkEndSeqNum(dmheader.sendSeq + dmheader.length);
        }
        else  // data length is zero in nondata message 
        {
        	socketObj.setchunkEndSeqNum(dmheader.sendSeq);
        }

        if (getServerOrClient() == MSocketConstants.CLIENT)
        {
          socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
        }
        
        if (dmheader.Type == DataMessage.DATA_ACK_REQ)
        {
          MSocketLogger.getLogger().fine("sending ACK Message for DATA_ACK_REQ");
          sendDataAckOnly(getConnID(), socketObj, dmheader.sendSeq);
        }
        else if (dmheader.Type == DataMessage.DATA_ACK_REP)
        {
          if (dmheader.ackSeq <= getObuffer().getDataBaseSeq())
          {
            numDupAckRecv++;
          }
          else
          {
            numDupAckRecv = 0;
          }
          
          getObuffer().setDataBaseSeq(dmheader.ackSeq);
          socketObj.setRecvdBytesOtherSide(dmheader.RecvdBytes);
          
          if (this.getMultipathWritingPolicy().getClass() == BlackBoxWritingPolicy.class)
          {
        	  // length carries the selective ack num
        	  int selectiveAckSeqNum = dmheader.length;
        	  ChunkInformation chunkInfo = new ChunkInformation(selectiveAckSeqNum, socketObj.getSocketIdentifer(), dmheader.RecvdBytes);
        	  ((BlackBoxWritingPolicy)this.getMultipathWritingPolicy()).informAckArrival(chunkInfo);
          }
          
          MSocketLogger.getLogger().fine("DATA_ACK_REP recv, setting data base seq num to " + dmheader.ackSeq + " act dataBaseseqnum "
              + getObuffer().getDataBaseSeq() + " dmheader.RecvdBytes " + dmheader.RecvdBytes + " SocketId "
              + socketObj.getSocketIdentifer() + " outstanding " + socketObj.getOutStandingBytes());
        }
        else if (dmheader.Type == DataMessage.KEEP_ALIVE)
        {
          socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
        }
        else if (dmheader.Type == DataMessage.CLOSE_FP)
        {
        	// dmheader.RecvdBytes is used to send num of CloseFP recvd from other side,
        	// on receiving three, both sides close and remove flowpath
        	long numCloseFPRecvdOtherSide = dmheader.RecvdBytes;
        	socketObj.setNumFPRecvdOtherSide((int)numCloseFPRecvdOtherSide);
        	socketObj.updateNumFPRecvd();
        	socketObj.setClosing();
        }
        else if ((dmheader.Type == DataMessage.FIN) || (dmheader.Type == DataMessage.ACK)
            || (dmheader.Type == DataMessage.ACK_FIN)) // any closed state
                                                       // machine message
        {
          processCloseStateMachineMessage(dmheader.Type);
        }
        else if(dmheader.Type == DataMessage.DATA_MESG)
        {
        	sendDataAckOnly(getConnID(), socketObj, dmheader.sendSeq);
        }
        
        nread = -2; // indicates that a header was successfully read
      }
    }

    long assrEnd = System.currentTimeMillis();
    MSocketInstrumenter.addActualSingleSample((assrEnd - assrStart));

    if (nread > 0)
    {
      // store read data in in buffer
      buf.flip();
      MSocketLogger.getLogger().fine("storing " + nread + " in inbuffer");

      long inbiStart = System.currentTimeMillis();
      InBufferStorageChunk InBObj = new InBufferStorageChunk(buf.array(), 0, socketObj.getChunkReadOffsetSeqNum(),
          nread);

      addInBuffer(InBObj);
      long inbiEnd = System.currentTimeMillis();

      MSocketInstrumenter.addInbufferInsertSample((inbiEnd - inbiStart));

      socketObj.updateChunkReadOffsetSeqNum(nread);
    }

    if (EOF)
    {
      nread = -1;
    }
    return nread;
  }

  private void processCloseStateMachineMessage(int messageType) throws IOException
  {
    switch (this.getMSocketState())
    {
      case MSocketConstants.FIN_WAIT_1 :
      {
        switch (messageType)
        {
          case DataMessage.FIN :
          {
            this.setMSocketState(MSocketConstants.CLOSING);
            setACKInOutbuffer(true); // simulating storing ACK in out buffer
            sendCloseAckOnly(this.getConnID());
            MSocketLogger.getLogger().fine("Close Message Encountered ACK sent in FIN_WAIT_1");
            break;
          }
          case DataMessage.ACK_FIN :
          {
            this.setMSocketState(MSocketConstants.TIME_WAIT);
            setACKInOutbuffer(true); // simulating storing ACK in out buffer
            sendCloseAckOnly(this.getConnID());
            MSocketLogger.getLogger().fine("ACK_FIN Encountered ACK sent in FIN_WAIT_1");
            MSocketLogger.getLogger().fine("wait for sometime and close the socket");
            internalClose();

            break;
          }
          case DataMessage.ACK :
          {
            MSocketLogger.getLogger().fine("ACK recv in FIN_WAIT_1");
            this.setMSocketState(MSocketConstants.FIN_WAIT_2);
            break;
          }
        }
        break;
      }

      case MSocketConstants.FIN_WAIT_2 :
      {
        if (messageType == DataMessage.FIN)
        {
          this.setMSocketState(MSocketConstants.TIME_WAIT);
          setACKInOutbuffer(true); // simulating storing ACK in out buffer
          sendCloseAckOnly(this.getConnID());
          MSocketLogger.getLogger().fine("FIN Encountered ACK sent in FIN_WAIT_2");
          MSocketLogger.getLogger().fine("wait for sometime and close the socket");
          internalClose();
        }
        break;
      }
      case MSocketConstants.CLOSING :
      {
        if (messageType == DataMessage.ACK)
        {
          this.setMSocketState(MSocketConstants.TIME_WAIT);
          MSocketLogger.getLogger().fine("wait for sometime and close the socket");
          internalClose();
        }
        break;
      }

      case MSocketConstants.LAST_ACK :
      {
        if (messageType == DataMessage.ACK)
        {
          internalClose();
          MSocketLogger.getLogger().fine(" close the socket");
        }
        break;
      }
      // incase of migrate reset recvd
      case MSocketConstants.CLOSED :
      {
        break;
      }

      case MSocketConstants.ACTIVE :
      {
        setMSocketState(MSocketConstants.CLOSE_WAIT);
        setACKInOutbuffer(true); // simulating storing ACK in out buffer
        sendCloseAckOnly(getConnID());
        MSocketLogger.getLogger().fine("Close Message Encountered ACK sent in ACTIVE state");
        break;
      }
    }
  }

  private SingleSocketReadReturnInfo singleSocketRead(SocketInfo socketObj, 
		  	byte[] b, int offset, int length)
	      throws IOException
	  {

	    long assrStart = System.currentTimeMillis();

	    SocketChannel dataChannel = socketObj.getDataChannel();

	    int nread = 0;
	    boolean EOF = false;
	    int ndirect = socketObj.canReadDirect();

	    int sizeRead = ndirect;

	    if (length < ndirect)
	    {
	      sizeRead = length;
	    }

	    ByteBuffer buf = ByteBuffer.allocate(sizeRead);

	    if (ndirect > 0)
	    {
	    	MSocketLogger.getLogger().fine("ndirect > 0");
	    	
	      int cur = 0;
	      cur = dataChannel.read(buf);

	      if (cur > 0)
	      { // needed because resendIf needed may prevet keep
	        // alive sending from server for more than 10 secs, threshold for proxy
	        // failure
	        if (getServerOrClient() == MSocketConstants.CLIENT)
	        {
	          socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
	        }
	      }

	      if (cur != -1)
	        nread += cur;
	      else
	        EOF = true;
	    }

	    else
	    { 
	    	// ndirect==0
	    	MSocketLogger.getLogger().fine("ndirect == 0 socket id "
	    			+socketObj.getSocketIdentifer());

	      long dmhStart = System.currentTimeMillis();
	      DataMessage dmheader = readDataMessageHeader(dataChannel);
	      long dmhEnd = System.currentTimeMillis();
	      MSocketLogger.getLogger().fine("readDataMessageHeader complete "
	    		  				+socketObj.getSocketIdentifer());

	      MSocketInstrumenter.addDataMessageHeaderSample(dmhEnd - dmhStart);

	      if (dmheader != null)
	      {
	    	  MSocketLogger.getLogger().fine(this.getServerOrClient()
	    			  		+" readDataMessageHeader complete not NULL "
	    			  		+socketObj.getSocketIdentifer());
	        socketObj.setChunkReadOffsetSeqNum(dmheader.sendSeq);
	        
	        // in DATA_ACK_REP, length field is selective ACK
	        if(dmheader.Type != DataMessage.DATA_ACK_REP)
	        {
	        	socketObj.setchunkEndSeqNum(dmheader.sendSeq + dmheader.length);
	        }
	        else  // data length is zero in nondata message 
	        {
	        	socketObj.setchunkEndSeqNum(dmheader.sendSeq);
	        }

	        if (getServerOrClient() == MSocketConstants.CLIENT)
	        {
	          socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
	          MSocketLogger.getLogger().fine("data message header read dmheader.sendSeq"
	        		+ dmheader.sendSeq
	      			+" dmheader.length "+dmheader.length);
	        } else
	        {
	        	//System.out.println("data message header read dmheader.sendSeq"+ dmheader.sendSeq
	        	//		+" dmheader.length "+dmheader.length);
	        }

	        if (dmheader.Type == DataMessage.DATA_ACK_REQ)
	        {
	          MSocketLogger.getLogger().fine("sending ACK Message for DATA_ACK_REQ");
	          sendDataAckOnly(getConnID(), socketObj, dmheader.sendSeq);
	        }
	        else if (dmheader.Type == DataMessage.DATA_ACK_REP)
	        {
	          if (dmheader.ackSeq <= getObuffer().getDataBaseSeq())
	          {
	            numDupAckRecv++;
	          }
	          else
	          {
	            numDupAckRecv = 0;
	          }

	          getObuffer().setDataBaseSeq(dmheader.ackSeq);
	          socketObj.setRecvdBytesOtherSide(dmheader.RecvdBytes);
	          
	          
	          if (this.getMultipathWritingPolicy().getClass() == BlackBoxWritingPolicy.class)
	          {
	        	  // length carries the selective ack num
	        	  int selecetiveAckNum = dmheader.length;
	        	  ChunkInformation chunkInfo 
	        	  		= new ChunkInformation(selecetiveAckNum, 
	        	  				socketObj.getSocketIdentifer(), dmheader.RecvdBytes);
	        	  ((BlackBoxWritingPolicy)
	        			  this.getMultipathWritingPolicy()).informAckArrival(chunkInfo);
	          }
	          
	          MSocketLogger.getLogger().fine("DATA_ACK_REP recv, setting data base seq num to " + dmheader.ackSeq 
	        		  + " act dataBaseSeqNum "
	              + getObuffer().getDataBaseSeq() + " dmheader.RecvdBytes " 
	        		  + dmheader.RecvdBytes + " SocketId "
	              + socketObj.getSocketIdentifer() + " outstanding " 
	        		  + socketObj.getOutStandingBytes()); 
	        }
	        else if (dmheader.Type == DataMessage.KEEP_ALIVE)
	        {
	          socketObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock());
	        }
	        else if (dmheader.Type == DataMessage.CLOSE_FP)
	        {
	        	long numCloseFPRecvdOtherSide = dmheader.RecvdBytes;
	        	socketObj.setNumFPRecvdOtherSide((int)numCloseFPRecvdOtherSide);
	        	socketObj.updateNumFPRecvd();
	        	socketObj.setClosing();
	        }
	        else if ((dmheader.Type == DataMessage.FIN) || (dmheader.Type == DataMessage.ACK)
	            || (dmheader.Type == DataMessage.ACK_FIN)) // any closed state
	                                                       // machine message
	        {
	          processCloseStateMachineMessage(dmheader.Type);
	        }
	        else if(dmheader.Type == DataMessage.DATA_MESG)
	        {
	        	sendDataAckOnly(getConnID(), socketObj, dmheader.sendSeq);
	        }
	        nread = -2; // indicates that a header was successfully read
	      }
	    }

	    long assrEnd = System.currentTimeMillis();
	    MSocketInstrumenter.addActualSingleSample((assrEnd - assrStart));

	    boolean copiedToApp = false;
	    int bytesCopiedToApp = 0;
	    if (nread > 0)
	    {
	      if (ibuffer.isDataInOrder(socketObj.getChunkReadOffsetSeqNum(), nread))
	      {
	        buf.flip();
	        bytesCopiedToApp = ibuffer.copyOrderedDataToAppBuffer(buf.array(), 
	        		socketObj.getChunkReadOffsetSeqNum(), nread,
	            b, offset, length);
	        copiedToApp = true;
	      }
	      else
	      {
	        // store read data in inbuffer
	        buf.flip();

	        long inbiStart = System.currentTimeMillis();
	        InBufferStorageChunk InBObj = new InBufferStorageChunk(buf.array(), 0, 
	        		socketObj.getChunkReadOffsetSeqNum(),
	            nread);

	        addInBuffer(InBObj);
	        long inbiEnd = System.currentTimeMillis();

	        MSocketInstrumenter.addInbufferInsertSample((inbiEnd - inbiStart));
	        copiedToApp = false;
	      }

	      socketObj.updateChunkReadOffsetSeqNum(nread);
	    }

	    SingleSocketReadReturnInfo retObj = null;

	    if (EOF)
	    {
	      nread = -1;
	      retObj = new SingleSocketReadReturnInfo(SingleSocketReadReturnInfo.MINUSONE, -1);
	    }
	    else if (nread == -2)
	    {
	      retObj = new SingleSocketReadReturnInfo(SingleSocketReadReturnInfo.DATAMESSAGEHEADER, -2);
	    }
	    else if (nread >= 0)
	    {
	      if (copiedToApp)
	      {
	        retObj = new SingleSocketReadReturnInfo(SingleSocketReadReturnInfo.COPIEDAPPBUFFER, 
	        		bytesCopiedToApp);
	      }
	      else
	      {
	        retObj = new SingleSocketReadReturnInfo(
	        		SingleSocketReadReturnInfo.COPIEDINPUTBUFFER, nread);
	      }
	    }
	    return retObj;
	  }
  
  public void attemptSocketWrite(SocketInfo Obj) throws IOException
  {
    Obj.getDataChannel().configureBlocking(false);

    byte[] writebuf = (byte[]) Obj.queueOperations(SocketInfo.QUEUE_GET, null);
    int curroffset = Obj.currentChunkWriteOffsetOper(-1, SocketInfo.VARIABLE_GET);
    ByteBuffer bytebuf = ByteBuffer.allocate(writebuf.length - curroffset);

    bytebuf.put(writebuf, curroffset, writebuf.length - curroffset);
    bytebuf.flip();
    long startTime = System.currentTimeMillis();
    int gotWritten = Obj.getDataChannel().write(bytebuf);

    if (gotWritten > 0)
    {
      MSocketLogger.getLogger().fine("gotWritten " + gotWritten + " buf length " + writebuf.length + " send buffer "
          + Obj.getSocket().getSendBufferSize() + " SocketID " + Obj.getSocketIdentifer());
      Obj.currentChunkWriteOffsetOper(gotWritten, SocketInfo.VARIABLE_UPDATE);
    }

    if (Obj.currentChunkWriteOffsetOper(-1, SocketInfo.VARIABLE_GET) == writebuf.length) // completely
                                                                                         // written,
                                                                                         // time
                                                                                         // to
                                                                                         // remove
                                                                                         // from
                                                                                         // head
                                                                                         // of
                                                                                         // queue
                                                                                         // and
                                                                                         // reset
                                                                                         // it
    {
      MSocketLogger.getLogger().fine("currentChunkWriteOffset " + writebuf.length);
      Obj.currentChunkWriteOffsetOper(0, SocketInfo.VARIABLE_SET);
      Obj.queueOperations(SocketInfo.QUEUE_REMOVE, null);
    }
    long endTime = System.currentTimeMillis();
    
    // wakeup the empty queue thread, 
    // it might have gone to sleep unlike the developer
    /*if( (Integer)Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0 )
    {
    	synchronized(this.getEmptyQueueThreadMonitor())
    	{
    		this.getEmptyQueueThreadMonitor().notify();
    	}
    }*/
    
    TemporaryTasksES.startTaskWithES(this, TemporaryTasksES.EMPTY_QUEUE);

    if (gotWritten > 0)
      MSocketLogger.getLogger().fine("Using socketID " + Obj.getSocketIdentifer() 
      + "Remote IP " + Obj.getSocket().getInetAddress()
          + "for writing " + " time taken " + (endTime - startTime));
  }

  /**
   * Migration type denotes Whether the IP and port is of server or client
   * Mobility Manager calls this, users need not call this,
   * 
   * @param rebindAddress
   * @param rebindPort
   * @param SocketId
   * @param MigrationType
   * @throws IOException
   * @throws InterruptedException
   */
  public boolean migrateSocketwithId(InetAddress rebindAddress, int rebindPort, 
		  int SocketId, int MigrationType)
  {
    synchronized (migrationMonitor)
    {
      MSocketLogger.getLogger().fine("migrateSocketwithId called with Id " + SocketId);

      MigrationTimeOutThread migThread = new MigrationTimeOutThread(this, SocketId);
      new Thread(migThread).start();

      boolean success = true;
      try
      {
        closeAll(SocketId);
        FlowPathResult res = addSocketToFlow(getConnID(), SetupControlMessage.MIGRATE_SOCKET, SocketId, rebindAddress,
            rebindPort, MigrationType);
        success = res.getSuccessful();

        MSocketLogger.getLogger().fine("Completed migrateSocketwithId " + SocketId);

      }
      catch (Exception ex)
      {
        success = false;
        MSocketLogger.getLogger().fine("excp in setupControlRead " + ex.getMessage());
      }
      // stop the thread as migration is not stuck in the setupcontrol read
      migThread.stopThread();
      return success;
    }
  }

  public void closeAll(int SocketId)
  {
    MSocketLogger.getLogger().fine("inside close");
    SocketInfo sockObj = getSocketInfo(SocketId);
    sockObj.setStatus(false);
    MSocketLogger.getLogger().fine("close done");
  }

  /**
   * Should be called only by SocketContoller, otherwise it will throw an
   * IOException. Used to initiate migration by the UDP controller
   * 
   * @param remoteAddress
   * @param remotePort
   * @throws Exception
   */

  /**
   * Should be called only by SocketContoller, otherwise it will throw an
   * IOException. Used to initiate migration by the UDP controller
   * 
   * @param remoteAddress
   * @param remotePort
   * @throws Exception
   */
  public void migrateRemote(InetAddress remoteAddress, int remotePort)
  {
    synchronized (migrateRemoteMonitor)
    {
      try
      {
        Vector<SocketInfo> vect = new Vector<SocketInfo>();
        vect.addAll(getAllSocketInfo());
        for (int i = 0; i < vect.size(); i++)
        {
          SocketInfo Obj = vect.get(i);
          addSocketToFlow(getConnID(), 
        		  SetupControlMessage.MIGRATE_SOCKET, Obj.getSocketIdentifer(),
              remoteAddress, remotePort, MSocketConstants.SERVER_MIG);
        }
      }
      catch (Exception ex)
      {
        MSocketLogger.getLogger().fine("migrateRemote excp");
        ex.printStackTrace();
      }
    }
  }

  public FlowPathResult addSocketToFlow(long flowID, int Operation, int socketId, InetAddress rebindAddress,
	      int rebindPort, int MigrationType)
	  {
	    synchronized (addSocketMonitor)
	    {
	      boolean success = true;
	      try
	      {
	        // check for new address from GNS, if it is GNS name, to handle simul
	        // mobility
	        if ((MigrationType == MSocketConstants.CLIENT_MIG) && (Operation == SetupControlMessage.MIGRATE_SOCKET))
	        {
	          if (typeOfCon == MSocketConstants.CON_TO_GNSNAME)
	          {
	            Random rand = new Random();
	            List<InetSocketAddress> socketAddressFromGNS = Integration.getSocketAddressFromGNS(serverAlias);
	            InetSocketAddress serverSock = socketAddressFromGNS.get(rand.nextInt(socketAddressFromGNS.size()));
	            serverIP = serverSock.getAddress();
	            serverPort = serverSock.getPort();
	          }
	          else if (typeOfCon == MSocketConstants.CON_TO_GNSGUID)
	          {
	            Random rand = new Random();
	            List<InetSocketAddress> socketAddressFromGNS = Integration.getSocketAddressFromGNS(serverAlias);
	            InetSocketAddress serverSock = socketAddressFromGNS.get(rand.nextInt(socketAddressFromGNS.size()));
	            serverIP = serverSock.getAddress();
	            serverPort = serverSock.getPort();
	          }
	        }

	        InetAddress ConnectIP = serverIP;
	        int ConnectPort = serverPort;

	        switch (Operation)
	        {
	          case SetupControlMessage.ADD_SOCKET :
	          {
	            Vector<InetAddress> Interfaces = CommonMethods.getActiveInterfaceInetAddresses();
	            SocketChannel NewChannel = SocketChannel.open();
	            NewChannel.configureBlocking(false);
	            if (rebindAddress != null)
	            {
	              NewChannel.socket().bind(new InetSocketAddress(rebindAddress, rebindPort));
	            }
	            else
	            {
	              if (Interfaces.size() > 0)
	              {
	                NewChannel.socket().bind(
	                    new InetSocketAddress(Interfaces.get(nextSocketIdentifier % Interfaces.size()), 0));
	              }
	            }

	            long connectStart = System.currentTimeMillis();
	            NewChannel.connect(new InetSocketAddress(ConnectIP, ConnectPort));
	            while (!NewChannel.finishConnect())
	              ;
	            long connectEnd = System.currentTimeMillis();
	            long connectTime = connectEnd - connectStart;

	            Socket NewSocket = NewChannel.socket();

	            MSocketLogger.getLogger().fine("Adding socket with Id " + nextSocketIdentifier + " to flow connected to server at "
	                + NewSocket.getInetAddress() + ":" + NewSocket.getPort() + "local IP " + NewSocket.getLocalAddress());

	            int UDPControllerPort = -1;
	            {
	              UDPControllerPort = UDPControllerHashMap.getUDPContollerPort(getControllerIP());
	            }

	            byte[] GUID = new byte[SetupControlMessage.SIZE_OF_GUID];
	            if (serverGUID.length() > 0)
	            {
	              GUID = CommonMethods.hexStringToByteArray(serverGUID);
	              MSocketLogger.getLogger().fine("serverGuid " + serverGUID + " GUID to be sent " + GUID + " length " + GUID.length);
	            }

	            long RTTStart = System.currentTimeMillis();
	            setupControlWrite(getControllerIP(), flowID, Operation, UDPControllerPort, NewChannel,
	                nextSocketIdentifier, -1, GUID, connectTime);
	            // Read remote port, address, and flowID
	            setupControlRead(NewChannel);
	            long RTTEnd = System.currentTimeMillis();
	            long estRTT = RTTEnd - RTTStart;

	            SocketInfo sockInfo = new SocketInfo(NewChannel, NewSocket, nextSocketIdentifier);
	            sockInfo.setEstimatedRTT(estRTT);

	            addSocketInfo(nextSocketIdentifier, sockInfo);
	            nextSocketIdentifier++;

	            inputQueuePutSocketInfo(sockInfo);
	            outputQueuePutSocketInfo(sockInfo);

	            break;
	          }
	          case SetupControlMessage.MIGRATE_SOCKET :
	          {
	            InetSocketAddress isaddr = null;
	            SocketChannel newChannel = null;
	            Socket newSocket = null;
	            if (MigrationType == MSocketConstants.CLIENT_MIG)
	            {
	              System.out.println("ConnectIP " + ConnectIP + " ConnectPort " + ConnectPort);
	              isaddr = new InetSocketAddress(ConnectIP, ConnectPort);

	              System.out.println("isaddr  " + isaddr.getAddress() + " isaddrPort " + isaddr.getPort()+
	            		  " rebindAddress "+rebindAddress);

	              newChannel = SocketChannel.open();
	              newSocket = newChannel.socket();

	              newSocket.bind(new InetSocketAddress(rebindAddress, rebindPort));

	              // impt bug resolved: even if connect or setup control blocks
	              // forever, even then subsequent migration will close the current
	              // channel
	              SocketInfo sockObj = getSocketInfo(socketId);
	              while (!sockObj.acquireLock())
	                ;
	              sockObj.setStatus(false);
	              sockObj.setSocketInfo(newChannel, newSocket);
	              sockObj.releaseLock();

	              sockObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock()); //

	              newChannel.connect(isaddr);
	              while (!newChannel.finishConnect())
	                ;
	              newSocket = newChannel.socket();

	              System.out.println("Reconnecing socket with Id " + socketId + " to flow connected to server at "
	                  + newSocket.getInetAddress() + ":" + newSocket.getPort() + "local IP " + newSocket.getLocalAddress());
	            }
	            else if (MigrationType == MSocketConstants.SERVER_MIG)
	            {
	              // TODO: here may be different choce of local interfaces
	              newChannel = SocketChannel.open();
	              newSocket = newChannel.socket();
	              // impt bug resolved: even if connect or setup control blocks
	              // forever, even then subsequent migration will close the current
	              // channel
	              SocketInfo sockObj = getSocketInfo(socketId);
	              while (!sockObj.acquireLock())
	                ;
	              sockObj.setStatus(false);
	              sockObj.setSocketInfo(newChannel, newSocket);
	              sockObj.releaseLock();

	              sockObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock()); //

	              MSocketLogger.getLogger().fine("set the newly created socket");

	              newChannel.connect(new InetSocketAddress(rebindAddress, rebindPort));
	              while (!newChannel.finishConnect())
	                ;
	              newSocket = newChannel.socket();
	              MSocketLogger.getLogger().fine("Reconnecing socket with Id " + socketId + " to flow connected to server at " + rebindAddress
	                  + ":" + rebindPort);
	              // updating server name and port
	              serverIP = rebindAddress;
	              serverPort = rebindPort;
	            }

	            SetupControlMessage scm = null;

	            int UDPControllerPort = -1;
	            {
	              try
	              {
	                UDPControllerPort = UDPControllerHashMap.getUDPContollerPort
	                		(getControllerIP());
	              }
	              catch (Exception ex)
	              {
	                MSocketLogger.getLogger().fine("UDP controller not properly set");
	                ex.printStackTrace();
	                UDPControllerPort = -1;
	              }
	            }
	            if (UDPControllerPort == -1)
	            {
	              MSocketLogger.getLogger().fine("MIGRATE_SOCKET UDPControllerPort " 
	            		  					+ UDPControllerPort);
	            }

	            byte[] GUID = new byte[SetupControlMessage.SIZE_OF_GUID];
	            if (serverGUID.length() > 0)
	            {
	              GUID = CommonMethods.hexStringToByteArray(serverGUID);
	              MSocketLogger.getLogger().fine("serverGuid " + serverGUID + " GUID to be sent " + GUID + " length "
	                  + CommonMethods.hexStringToByteArray(serverGUID).length);
	            }

	            setupControlWrite(getControllerIP(), flowID, Operation, UDPControllerPort, newChannel,
	                socketId, -1, GUID, 0);
	            // Read remote port, address, and flowID
	            scm = setupControlRead(newChannel);

	            if (scm.mesgType == SetupControlMessage.MIGRATE_SOCKET_RESET)
	            {
	              MSocketLogger.getLogger().fine("MIGRATE_SOCKET_RESET recvd");
	              internalClose();
	              throw new Exception("Reset received");
	            }
	            
	            getObuffer().setDataBaseSeq(scm.ackSeq);
	            
	            SocketInfo sockObj = getSocketInfo(scm.socketID);
	            
	            while (!sockObj.acquireLock());
	            
	            sockObj.setSocketInfo(newChannel, newSocket);
	            sockObj.setStatus(true); // true means active
	                                     // again
	            sockObj.setLastKeepAlive(KeepAliveStaticThread.getLocalClock()); // making
	                                                       // it
	                                                       // active
	                                                       // again
	            sockObj.releaseLock();
	            
	            inputQueuePutSocketInfo(sockObj);
	            outputQueuePutSocketInfo(sockObj);
	            
	            MSocketLogger.getLogger().fine("set the new socket");
	            
	            synchronized (getSocketMonitor())
	            {
	              getSocketMonitor().notifyAll(); // waking up blocked threads
	            }
	            
	            setupClientController(scm);
	            
	            ResendIfNeededThread RensendObj = new ResendIfNeededThread(this);
	            (new Thread(RensendObj)).start();
	            break;
	          }
	        }
	      }
	      catch (Exception ex)
	      {
	        success = false;
	        MSocketLogger.getLogger().fine("exception in addFlow " + ex.getMessage());
	      }
	      // -1 because it is incremented by 1;
	      FlowPathResult Obj = new FlowPathResult(nextSocketIdentifier - 1, success);
	      return Obj;
	    }
	  }

  /**
   * Removes socket/flowpath from the flow/connection.
   * @param socketId
   */
  public void removeSocketFromFlow(int socketId)
  {
    /*SocketInfo socketObj = removeSocketInfo(socketId);
    while (!socketObj.acquireLock());
    socketObj.setStatus(false);
    socketObj.releaseLock();*/
	
	SocketInfo socketObj = getSocketInfo(socketId);
	while (!socketObj.acquireLock());
    socketObj.setClosing();
    socketObj.releaseLock();
  }

  public void setupClientController(SetupControlMessage scm)
  {
    MSocketLogger.getLogger().fine("Received IP:port " + scm.port + ":" + scm.iaddr + "; ackSeq = " + scm.ackSeq);
    setRemoteControlAddress(scm.iaddr);
    setRemoteControlPort(scm.port);
  }

  
  public void internalClose()
  {
    if (serverOrClient == MSocketConstants.CLIENT)
    {
      MSocketLogger.getLogger().fine("unregistering with mobility manager and udp controller");
      MobilityManagerClient.unregisterWithManager(this);
      UDPControllerHashMap.unregisterWithController(getControllerIP(), this);
    }

    try
    {
      setMSocketState(MSocketConstants.CLOSED);
      closeAll();

      while (!setState(ConnectionInfo.ALL_READY, true))
      {

      }
      MSocketLogger.getLogger().fine("MSocket in CLOSED state");
      releaseOutBuffer();
      timerRunning = false;
      
      // unregister to send keep alives
      KeepAliveStaticThread.unregisterForKeepAlive(this);
      
      // writer may get unblocked
      setblockingFlag(false);

      // reader may get unblocked and throw exception
      synchronized (getSocketMonitor())
      {
        getSocketMonitor().notifyAll(); // waking up blocked threads
      }

      // close the selectors
      this.getInputStreamSelector().close();
      this.getOutputStreamSelector().close();

      /*if(backWritingThread != null)
    	  this.backWritingThread.stopRetransmissionThread();
      
      synchronized (getBackgroundThreadMonitor())
      {
        getBackgroundThreadMonitor().notifyAll();
      }
      
      if(this.emptyQueueThread != null)
      {
    	  this.emptyQueueThread.stopThread();
      }
      
      synchronized (this.getEmptyQueueThreadMonitor())
      {
    	  getEmptyQueueThreadMonitor().notify();
      }*/
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }

    if (serverOrClient == MSocketConstants.SERVER)
    {
//      ServerMSocket ims = (ServerMSocket) msocket;
//      ims.removeFlowId();
      
      serverController.removeConnectionInfo(getConnID());
    }
  }

  public void addSocketHashMap(SocketChannel NewChannel, long estRTT)
  {
    SocketInfo sockInfo = new SocketInfo(NewChannel, NewChannel.socket(), nextSocketIdentifier);
    sockInfo.setEstimatedRTT(estRTT);
    addSocketInfo(nextSocketIdentifier, sockInfo);
    nextSocketIdentifier++;
    
    // set alreay defined user buffers
    if(this.userSetSendBufferSize != 0)
    {
    	try {
			NewChannel.socket().setSendBufferSize(this.userSetSendBufferSize);
		} catch (SocketException e) {
			e.printStackTrace();
		}
    }

    // registering with the selector
    inputQueuePutSocketInfo(sockInfo);
    outputQueuePutSocketInfo(sockInfo);
  }

  private void setupControlWrite(InetAddress ControllerAddress, long lfid, int mstype, int ControllerPort,
	      SocketChannel SCToUse, int SocketId, int ProxyId, byte[] GUID, long connectTime)
	      throws IOException
	  {
	    int DataAckSeq = 0;
	    DataAckSeq = getDataAckSeq();
	    if (mstype == SetupControlMessage.NEW_CON_MESG || mstype == SetupControlMessage.ADD_SOCKET)
	    {
	      // connect Time overloaded
	      DataAckSeq = (int) connectTime;
	    }

	    SetupControlMessage scm = new SetupControlMessage(ControllerAddress, ControllerPort, lfid, DataAckSeq, mstype,
	        SocketId, ProxyId, GUID);
	    ByteBuffer buf = ByteBuffer.wrap(scm.getBytes());

	    while (buf.remaining() > 0)
	    {
	      SCToUse.write(buf);
	    }

	    MSocketLogger.getLogger().fine("Sent IP:port " + ControllerPort + "; ackSeq = " + DataAckSeq);
	  }

  private SetupControlMessage setupControlRead(SocketChannel SCToUse) throws IOException
  {
    ByteBuffer buf = ByteBuffer.allocate(SetupControlMessage.SIZE);

    int ret = 0;
    while (buf.position() < SetupControlMessage.SIZE)
    {
      MSocketLogger.getLogger().fine("setup control read happening");
      ret = SCToUse.read(buf);
      MSocketLogger.getLogger().fine("setup control read returned");
      if (ret == -1)
      {
        MSocketLogger.getLogger().fine("setup control read -1 returned");
        if (buf.position() < SetupControlMessage.SIZE)
        {
          MSocketLogger.getLogger().fine("setup control read throwing exception");
          throw new IOException("setupControlRead failed");
        }
      }
    }

    SetupControlMessage scm = SetupControlMessage.getSetupControlMessage(buf.array());
    return scm;
  }

  public void setServerAlias(String alias)
  {
    this.serverAlias = alias;
  }

  public String getServerAlias()
  {
    return this.serverAlias;
  }

  public void setServerIP(InetAddress servIP)
  {
    this.serverIP = servIP;
  }

  public InetAddress getServerIP()
  {
    return this.serverIP;
  }

  public void setServerPort(int serverPort)
  {
    this.serverPort = serverPort;
  }

  public int getServerPort()
  {
    return this.serverPort;
  }

  public void setTypeOfCon(int typeOfCon)
  {
    this.typeOfCon = typeOfCon;
  }

  public int getTypeOfCon()
  {
    return this.typeOfCon;
  }

  public void setServerGUID(String stringGUID)
  {
    this.serverGUID = stringGUID;
  }

  public String getServerGUID()
  {
    return this.serverGUID;
  }


  /**
   * To set TCP no delay on all the active sockets
   * 
   * @param on
   * @throws SocketException
   */
  public void setTcpNoDelay(boolean on) throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setTcpNoDelay(on);
      }
    }
  }

  /**
   * Sets so linger on all the sockets
   * 
   * @param on
   * @param linger
   * @throws SocketException
   */
  public void setSoLinger(boolean on, int linger) throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setSoLinger(on, linger);
      }
    }
  }

  /**
   * Sets send-buffer on all the sockets
   * 
   * @param size
   * @throws SocketException
   */
  public void setSendBufferSize(int size) throws SocketException
  {
	this.userSetSendBufferSize = size;
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setSendBufferSize(size);
      }
    }
  }

  /**
   * returns sum of send buffer size among all active flowpaths
   * 
   * @param size
   * @throws SocketException
   */
  public int getSendBufferSize() throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    int maxSize = -1;
    int sumSize = 0;
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        if (maxSize == -1)
        {
          maxSize = Obj.getSocket().getSendBufferSize();
        }
        else if (maxSize < Obj.getSocket().getSendBufferSize())
        {
          maxSize = Obj.getSocket().getSendBufferSize();
        }
        sumSize += Obj.getSocket().getSendBufferSize();
      }
    }
    return sumSize;
  }

  /**
   * returns sum of send buffer size among all active flowpaths
   * 
   * @param size
   * @throws SocketException
   */
  public int getReceiveBufferSize() throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    int maxSize = -1;
    int sumSize = 0;
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if ( Obj.getStatus() )
      {
        if (maxSize == -1)
        {
          maxSize = Obj.getSocket().getReceiveBufferSize();
        }
        else if (maxSize < Obj.getSocket().getReceiveBufferSize())
        {
          maxSize = Obj.getSocket().getReceiveBufferSize();
        }
        sumSize += Obj.getSocket().getReceiveBufferSize();
      }
    }
    return sumSize;
  }

  /**
   * sets the recv buffer size on all the active flowpaths, between the server
   * and the client.
   * 
   * @param size
   * @throws SocketException
   */
  public void setReceiveBufferSize(int size) throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setReceiveBufferSize(size);
      }
    }
  }

  /**
   * Sets the Keep alive on all active flowpaths, between the server and the
   * client.
   * 
   * @param on
   * @throws SocketException
   */
  public void setKeepAlive(boolean on) throws SocketException
  {

    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setKeepAlive(on);
      }
    }
  }

  /**
   * sets the traffic class on all the active flow apths, between the server and
   * the client
   * 
   * @param tc
   * @throws SocketException
   */
  public void setTrafficClass(int tc) throws SocketException
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setTrafficClass(tc);
      }
    }
  }

  /**
   * Sets the performance preferences for all the active flowpath, between the
   * server and the client.
   * 
   * @param connectionTime
   * @param latency
   * @param bandwidth
   */
  public void setPerformancePreferences(int connectionTime, int latency, int bandwidth)
  {
    Vector<SocketInfo> vect = new Vector<SocketInfo>();
    vect.addAll(getAllSocketInfo());
    for (int i = 0; i < vect.size(); i++)
    {
      SocketInfo Obj = vect.get(i);
      // if the socket is active
      if (Obj.getStatus())
      {
        Obj.getSocket().setPerformancePreferences(connectionTime, latency, bandwidth);
      }
    }
  }

  public void blockOnInputStreamSelector()
  {
    MSocketLogger.getLogger().fine(this.getServerOrClient() + " blockOnInputStreamSelector called");
    while (true)
    {
      // check for the queue, if there are any channels to register
      while (inputQueueGetSize() != 0)
      {
        SocketInfo regSocket = (SocketInfo) inputQueueGetSocketInfo();
        SelectionKey SelecKey;
        try
        {
          MSocketLogger.getLogger().fine(this.getServerOrClient() + "registering keys in the selector");
          regSocket.getDataChannel().configureBlocking(false);
          SelecKey = regSocket.getDataChannel().register(getInputStreamSelector(), SelectionKey.OP_READ);
          SelecKey.attach(regSocket);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }

      int readyChannels = 0;
      try
      {
        MSocketLogger.getLogger().fine(this.getServerOrClient() + "blocked on the selector");
        readyChannels = getInputStreamSelector().select();
      }
      catch (Exception e)
      {
        //e.printStackTrace();
        MSocketLogger.getLogger().fine(e.getMessage());
        // if selector not open, then break
        if (!getInputStreamSelector().isOpen())
        {
          break;
        }
      }

      if (readyChannels == 0)
      {
        continue;
      }
      else
      {
        MSocketLogger.getLogger().fine(this.getServerOrClient() + "unblocked on the selector");
        Set<SelectionKey> selectedKeys = getInputStreamSelector().selectedKeys();
        selectedKeys.clear();
        break;
      }
    }
  }

  public void blockOnOutputStreamSelector()
  {
    MSocketLogger.getLogger().fine(this.getServerOrClient() + " blockOnOutputStreamSelector called");
    while (true)
    {
      // check for the queue, if there are any channels to register
      while (outputQueueGetSize() != 0)
      {
        SocketInfo regSocket = outputQueueGetSocketInfo();
        SelectionKey SelecKey;
        try
        {
          MSocketLogger.getLogger().fine(this.getServerOrClient() + "registering keys in the selector");
          regSocket.getDataChannel().configureBlocking(false);
          SelecKey = regSocket.getDataChannel().register(getOutputStreamSelector(), SelectionKey.OP_WRITE);
          SelecKey.attach(regSocket);
        }
        catch (Exception e)
        {
          MSocketLogger.getLogger().fine(e.getMessage());
        }
      }

      int readyChannels = 0;
      try
      {
        MSocketLogger.getLogger().fine(this.getServerOrClient() + "blocked on the selector");
        readyChannels = getOutputStreamSelector().select(); // changing it
                                                            // to select(),
                                                            // makes it
                                                            // blocking,
                                                            // then it
                                                            // deadlocks
                                                            // with
                                                            // register()
      }
      catch (Exception e)
      {
        // e.printStackTrace();
        MSocketLogger.getLogger().fine(e.toString());

        // if not open then break from loop
        if (!getOutputStreamSelector().isOpen())
        {
          break;
        }
      }

      if (readyChannels == 0)
      {
        continue;
      }
      else
      {
        MSocketLogger.getLogger().fine(this.getServerOrClient() + "unblocked on the selector");
        Set<SelectionKey> selectedKeys = getOutputStreamSelector().selectedKeys();
        selectedKeys.clear();
        break;
      }
    }
  }

  private void checkToStartDataAckThread(SocketInfo Obj)
  {
    MSocketLogger.getLogger().fine("checkToStartDataAckThread called ");
    if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
    {
      //attemptSocketWrite(Obj);
      return;
    }

    if (!notAckedInAWhile(Obj)) // not flooding ACKs to sender
      return;

    MSocketLogger.getLogger().fine("checkToStartDataAckThread starting the thread ");
    SendDataAckThread tsd = new SendDataAckThread(this, Obj);
    new Thread(tsd).start();
  }

  /**
   * Empties the write queues. Returning from Background writing before emptying
   * will desynchronize the streams
   */
  public void emptyTheWriteQueues()
  {
    Vector<SocketInfo> socketList = new Vector<SocketInfo>();
    socketList.addAll(getAllSocketInfo());
    while (true)
    {
      boolean runAgain = false;
      blockOnOutputStreamSelector();
      for (int i = 0; i < socketList.size(); i++)
      {
        SocketInfo Obj = socketList.get(i);
        if (Obj.getStatus())
        {
          while (!Obj.acquireLock())
            ;
          if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
          {
        	try
        	{
        		attemptSocketWrite(Obj);
        	} catch(IOException sx)
        	{
        		sx.printStackTrace();
        	}
            runAgain = true;
          }
          Obj.releaseLock();
        }
      }
      if (!runAgain)
      {
        break;
      }
    }
  }
  
  /**
   * Attempt to empty the write queue, it is a no-blocking
   * operation.
   */
  public void attemptToEmptyTheWriteQueues()
  {
    Vector<SocketInfo> socketList = new Vector<SocketInfo>();
    socketList.addAll(getAllSocketInfo());
   
      for (int i = 0; i < socketList.size(); i++)
      {
        SocketInfo Obj = socketList.get(i);
        if (Obj.getStatus())
        {
          while (!Obj.acquireLock());
          
          if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
          {
        	try
        	{  
        		attemptSocketWrite(Obj);
        	} catch(IOException ex)
        	{
        		ex.printStackTrace();
        	}
          }
          Obj.releaseLock();
          
        }
      }
  }

  /**
   * Returns the obuffer value.
   * 
   * @return Returns the obuffer.
   */
  public OutBuffer getObuffer()
  {
    return obuffer;
  }

  /**
   * Returns the inputStreamSelectorMonitor value.
   * 
   * @return Returns the inputStreamSelectorMonitor.
   */
  public Object getInputStreamSelectorMonitor()
  {
    return inputStreamSelectorMonitor;
  }

  /**
   * Returns the backgroundThreadMonitor value.
   * 
   * @return Returns the backgroundThreadMonitor.
   */
  //public Object getBackgroundThreadMonitor()
  //{
  //  return backgroundThreadMonitor;
  //}
  
  /**
   * 
   * Returns emptyQueueThreadMonitor.
   * 
   * @return
   */
  //public Object getEmptyQueueThreadMonitor()
  //{
  //	  return emptyQueueThreadMonitor;
  //}

  /**
   * Returns the blockingFlagMonitor value.
   * 
   * @return Returns the blockingFlagMonitor.
   */
  public Object getBlockingFlagMonitor()
  {
    return blockingFlagMonitor;
  }

  /**
   * Returns the socketMonitor value.
   * 
   * @return Returns the socketMonitor.
   */
  public Object getSocketMonitor()
  {
    return socketMonitor;
  }

  /**
   * Returns the controllerIP value.
   * 
   * @return Returns the controllerIP.
   */
  public InetAddress getControllerIP()
  {
    return controllerIP;
  }

  /**
   * Sets the controllerIP value.
   * 
   * @param controllerIP The controllerIP to set.
   */
  public void setControllerIP(InetAddress controllerIP)
  {
    this.controllerIP = controllerIP;
  }

  private class MigrationTimeOutThread implements Runnable
  {
    long           startTime = 0;
    boolean        running   = true;
    ConnectionInfo cinfo     = null;
    int            socketID  = -1;

    MigrationTimeOutThread(ConnectionInfo cinfo, int socketID)
    {
      this.startTime = System.currentTimeMillis();
      this.cinfo = cinfo;
      this.socketID = socketID;
    }

    @Override
    public void run()
    {
      while (running)
      {
        long currTime = System.currentTimeMillis();
        if ((currTime - startTime) > MIGRATION_TIMEOUT)
        {
          cinfo.closeAll(socketID);
          break;
        }

        try
        {
          Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
        }
      }
      MSocketLogger.getLogger().fine("MigrationTimeOutThread exits");
    }

    public void stopThread()
    {
      running = false;
    }
  }

  /**
   * This thread only sends back data ack, when the receiver has received the
   * data and not reading any more, but the receiver has not sent the ack back
   * to the sender. Required for default policy, otherwise the background thread
   * doesn't stop, as it deosn't get the ack back from the receiver
   * 
   * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
   * @version 1.0
   */
  private class SendDataAckThread implements Runnable
  {
    ConnectionInfo cinfo = null;
    SocketInfo     Obj   = null;

    SendDataAckThread(ConnectionInfo cinfo, SocketInfo Obj)
    {
      this.cinfo = cinfo;
      this.Obj = Obj;
    }

    @Override
    public void run()
    {
      MSocketLogger.getLogger().fine("SendDataAckThread acquiring READ_WRITE");
      boolean ret = cinfo.setState(ConnectionInfo.READ_WRITE, true); // blocking
                                                                     // acquire
      if (ret)
      {

        try
        {
          MSocketLogger.getLogger().fine("SendDataAckThread sending data ack");
          sendDataAckOnly(cinfo.getConnID(), Obj, 0);
        }
        catch (Exception ex)
        {
          MSocketLogger.getLogger().fine("exception in SendDataAckThread ");
        }
        cinfo.setState(ConnectionInfo.ALL_READY, true);
      }
    }
  }

}