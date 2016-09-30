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

import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class keeps the information about each flowpath. It stores the socket,
 * dataChannel sequence number of data flowing through that flowpath.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */

public class SocketInfo
{
  public static final int       QUEUE_GET                      = 1;
  public static final int       QUEUE_PUT                      = 2;
  public static final int       QUEUE_REMOVE                   = 3;
  public static final int       QUEUE_SIZE                     = 4;

  public static final int       VARIABLE_UPDATE                = 1;
  public static final int       VARIABLE_SET                   = 2;
  public static final int       VARIABLE_GET                   = 3;

  public final Object           queueMonitor                   = new Object();
  public final Object           byteInfoVectorMonitor          = new Object();
  public final Object           currentChunkWriteOffsetMonitor = new Object();

  private SocketChannel         dataChannel                    = null;
  private Socket                socket                         = null;
  private int                   SocketIdentifier               = -1;
  
  // offset, in seq num, of data read from the current chunk
  private long                  chunkReadOffsetSeqNum          = 0;
  /*
   * how much ahead is the next data message header, before that data is there
   * End seq num of current chunk
   */
  private int                   chunkEndSeqNum                 = 0;

  private boolean               active                         = true;
  /*
   * total bytes sent on this socket since beginning
   */
  private long                  numBytesSent                   = 0;
  /*
   * total bytes Recvd by on this socket since beginning
   */
  private long                  numBytesRecv                   = 0;
  /*
   * total bytes Recvd by other side on this socket since beginning, updated on
   * ACK
   */
  private long                  numBytesRecvOtherSide          = 0;
  /*
   * lock to synchronize state change between write exception and migrate call
   */
  private boolean               stateChangeLock                = false;

  private long                  lastKeepAlive                  = 0;
  private Queue<byte[]>         sendingQueue                   = null;
  /*
   * due to non-blocking writes current chunk may not be written completely in
   * one go. so this variable maintain the offset till which it's been written.
   */
  private int                   currentChunkWriteOffset        = 0;
  
  // keeps track of num of bytes recv since the last ack was sent
  private long lastNumBytesRecv								   = 0;
  
  // stores the seq num till which data has been sent in handle migration case
  private long handleMigSeqNum 								   = 0;
  
  // to store byte ranges
  private Vector<ByteRangeInfo> byteInfoVector;

  private final Object          socketLockMonitor              = new Object();
  
  // to request ACK after migration
  private boolean needToRequestACK							   = false;
  
  private long estimatedRTT								   	   = 0;
  
  // keeps track of how many more can be sent over it. According to Thpt
  // ratio.
  private long numRemChunks							   		   = 0;
  
  // indicates if app has requested to remove this flowpath
  // or it has received a request from other side
  private boolean inClosing									   = false;
  
  // true indicates that this flowpath was closed by remove flowpath
  // method
  private boolean closed									   = false;
  
  //true indicates that this flowpath was closed by remove flowpath
  // method
  private int numCloseFPRecvd								   = 0;
  
  private int numCloseFPRecvdOtherSide						   = 0;
  
  public SocketInfo(SocketChannel dataChannel, Socket socket, int SocketIdentifier)
  {
    this.dataChannel = dataChannel;
    this.socket = socket;
    this.SocketIdentifier = SocketIdentifier;
    chunkReadOffsetSeqNum = 0; // may not be related to sending seq num
    chunkEndSeqNum = 0;        // may not be related to sending seq num
    active = true;
    sendingQueue = new LinkedList<byte[]>();
    byteInfoVector = new Vector<ByteRangeInfo>();
  }

  public Object queueOperations(int operType, byte[] putObject)
  {
    synchronized (queueMonitor)
    {
      switch (operType)
      {
        case QUEUE_GET :
        {
          return sendingQueue.peek();
        }
        case QUEUE_PUT :
        {
          sendingQueue.add(putObject);
          break;
        }
        case QUEUE_REMOVE :
        {
          sendingQueue.remove();
          break;
        }
        case QUEUE_SIZE :
        {
          return sendingQueue.size();
        }
      }
      return null;
    }
  }

  public Object byteInfoVectorOperations(int oper, long startSeqNum, int length)
  {
    synchronized (byteInfoVectorMonitor)
    {
      switch (oper)
      {
        case QUEUE_GET :
        {
          Vector<ByteRangeInfo> retVector = new Vector<ByteRangeInfo>();
          retVector.addAll(byteInfoVector);
          return retVector;
        }
        case QUEUE_PUT :
        {
          ByteRangeInfo Obj = new ByteRangeInfo(startSeqNum, length, SocketIdentifier);
          byteInfoVector.add(Obj);
          break;
        }
        case QUEUE_REMOVE :
        {
        	// start SeqNum acts as database seq num or ack num. 
        	// previous byte ranges can be removed
			freeByteRanges(startSeqNum);
        }
        case QUEUE_SIZE :
        {
        }
      }
      return null;
    }
  }

  public int currentChunkWriteOffsetOper(int value, int oper)
  {
    synchronized (currentChunkWriteOffsetMonitor)
    {
      switch (oper)
      {
        case VARIABLE_UPDATE :
        {
          currentChunkWriteOffset += value;
          break;
        }
        case VARIABLE_SET :
        {
          currentChunkWriteOffset = value;
          break;
        }
        case VARIABLE_GET :
        {
          return currentChunkWriteOffset;
        }
      }
      return -1;
    }
  }

  /**
   * Set dataChannel and socket, also resets various counters
   * 
   * @param dataChannel
   * @param socket
   */
  public synchronized void setSocketInfo(SocketChannel dataChannel, Socket socket)
  {
    // while deactivating reset the databoundaryseq and data Ack
	chunkReadOffsetSeqNum = 0;
	chunkEndSeqNum = 0;

    this.dataChannel = dataChannel;
    this.socket = socket;

    numBytesSent = 0;
    numBytesRecv = 0;
    numBytesRecvOtherSide = 0;
    lastKeepAlive = 0;
    
    // no pending chunks
 	currentChunkWriteOffset=0;

    sendingQueue.clear();
    
    // do not clear byInfoVector, as it stores chunk needs to be sent in migration
 	//byteInfoVector.clear();
  }

  /**
   * lock to update critical info
   * 
   * @return
   */
  public boolean acquireLock()
  {
    synchronized (socketLockMonitor)
    {
      if (stateChangeLock == false)
      {
        stateChangeLock = true;
        return true;
      }
      else
      {
        while (stateChangeLock == true)
        {
          try
          {
            socketLockMonitor.wait();
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
          }
        }
        stateChangeLock = true;
        return true;
      }
    }
  }

  /**
   * release the lock after updating critical info
   */
  public void releaseLock()
  {
    stateChangeLock = false;
    synchronized (socketLockMonitor)
    {
      socketLockMonitor.notifyAll();
    }
  }

  /**
   * socket active/working or not, true : working, false: not working
   * 
   * @return
   */
  public boolean getStatus()
  {
    return active;
  }

  public synchronized void setStatus(boolean status)
  {
    active = status;
    MSocketLogger.getLogger().fine("inside set status");
    if (status == false)
    {
      try
      {
        socket.close();
        dataChannel.close();
      }
      catch (Exception ex)
      {
        ex.printStackTrace();
        MSocketLogger.getLogger().fine("exceptio in closing during status set");
      }
    }
  }

  public Socket getSocket()
  {
    return socket;
  }

  public SocketChannel getDataChannel()
  {
    return dataChannel;
  }

  public int getSocketIdentifer()
  {
    return SocketIdentifier;
  }

  /**
   * tells how much bytes can be read without encountering data message header
   * 
   * @return
   */
  public int canReadDirect()
  {
    return (int) (chunkEndSeqNum - chunkReadOffsetSeqNum);
  }

  public synchronized void setchunkEndSeqNum(int s)
  {
	  chunkEndSeqNum = s;
  }
  
  public  int  getChunkEndSeqNum()
  {
	  return chunkEndSeqNum;
  }

  public synchronized void updateChunkReadOffsetSeqNum(int s)
  {
	  chunkReadOffsetSeqNum += s;
  }

  /**
   * sometimes need to set it to next seq num as it may not be contigous
   * segment,
   */
  public synchronized void setChunkReadOffsetSeqNum(int s)
  {
	  chunkReadOffsetSeqNum = s;
  }

  public int getChunkReadOffsetSeqNum()
  {
    return (int) chunkReadOffsetSeqNum;
  }

  public synchronized void updateSentBytes(long toBeAdded)
  {
    numBytesSent += toBeAdded;
    MSocketLogger.getLogger().fine("SentBytes Updated " + numBytesSent + " SocketID " + SocketIdentifier);
  }

  public synchronized void updateRecvdBytes(long toBeAdded)
  {
    numBytesRecv += toBeAdded;
  }
  
  public synchronized void setLastNumBytesRecv() {
		lastNumBytesRecv = numBytesRecv;
	}
  
  public synchronized void setHandleMigSeqNum(long handleMigSeqNum) {
		this.handleMigSeqNum = handleMigSeqNum;
	}
	
	public long getHandleMigSeqNum() {
		return this.handleMigSeqNum;
	}

  public synchronized void setRecvdBytesOtherSide(long RecvdBytes)
  {
    if (RecvdBytes > numBytesRecvOtherSide)
      numBytesRecvOtherSide = RecvdBytes;
  }
  
  public long getLastNumBytesRecv() 
  {
		return lastNumBytesRecv;
  }

  public long getSentBytes()
  {
    return numBytesSent;
  }

  public long getRecvdBytes()
  {
    return numBytesRecv;
  }

  public long getRecvdBytesOtherSide()
  {
    return numBytesRecvOtherSide;
  }

  public long getOutStandingBytes()
  {
    return (numBytesSent - numBytesRecvOtherSide);
  }

  public double getOutStandingBytesRatio()
  {
    double rt = 0;
    if (numBytesRecvOtherSide != 0)
    {
      rt = (numBytesSent - numBytesRecvOtherSide) / numBytesRecvOtherSide;
    }
    return rt;
  }

  public void setLastKeepAlive(long lastKeepAlive)
  {
    if (lastKeepAlive > this.lastKeepAlive)
    {
      this.lastKeepAlive = lastKeepAlive;
    }
  }

  public long getLastKeepAlive()
  {
    return this.lastKeepAlive;
  }
  
  public boolean getneedToReqeustACK() 
  {
  	return needToRequestACK;
  }

  public void setneedToReqeustACK( boolean value ) 
  {
		// each time this flag changes reset it to 0
		handleMigSeqNum = 0;
		needToRequestACK = value;
  }
  
  public void setEstimatedRTT(long updateRTT)
  {
	  estimatedRTT =  updateRTT;
  }
  
  public long getEstimatedRTT()
  {
	  return estimatedRTT;
  }
  
  public long getNumRemChunks()
  {
	  return numRemChunks;
  }
  
  public void decrementNumRemChunks()
  {
	  numRemChunks--;
  }
  
  public void setNumRemChunks(long l) 
  {
	  this.numRemChunks = l;
  }
  
  /*
   * is to true if the flowpath is closing
   */
  public void setClosing()
  {
	inClosing = true;  
  }
  
  public boolean getClosing()
  {
	return inClosing;  
  }
  
  public void updateNumFPRecvd()
  {
	  this.numCloseFPRecvd++;
  }
  
  public int getNumFPRecvd()
  {
	  return this.numCloseFPRecvd;
  }
  
  public int getNumFPRecvdOtherSide()
  {
	  return this.numCloseFPRecvdOtherSide;
  }
  
  public void setNumFPRecvdOtherSide(int numFPOtherSide)
  {
	  this.numCloseFPRecvdOtherSide = numFPOtherSide;
  }
  
  private void freeByteRanges( long dataBaseSeqNum )
  {
	  int freeIndex=-1;
	  for( int i=0; i<byteInfoVector.size(); i++ ) 
	  {
		  ByteRangeInfo curObj = byteInfoVector.get(i);
		  // required for considering holes ,
		  // FIXME: may not have checked for repeated data
		  if( ( dataBaseSeqNum > curObj.getStartSeqNum() ) && 
				  ( dataBaseSeqNum <= (curObj.getStartSeqNum()+curObj.getLength()) ) ) 
		  {
			  freeIndex=i;
			  break;
		  }
	  }
			
	  int i=0;
	  while( i < freeIndex )
	  {
		  byteInfoVector.remove(0);  //remove the first element, 
		  							// as element slides left
		  i++;
	  }
  }
  
}