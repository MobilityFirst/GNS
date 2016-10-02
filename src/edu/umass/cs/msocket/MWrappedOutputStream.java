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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import edu.umass.cs.msocket.common.policies.BlackBoxWritingPolicy;
import edu.umass.cs.msocket.common.policies.FullDuplicationWritingPolicy;
import edu.umass.cs.msocket.common.policies.MultipathWritingPolicy;
import edu.umass.cs.msocket.common.policies.RTTBasedWritingPolicy;
import edu.umass.cs.msocket.common.policies.RoundRobinWritingPolicy;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * This class implements the Output stream of the MSocket
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */

public class MWrappedOutputStream extends OutputStream
{
  /**
   * Size of a write block
   */
  public static final int      WRITE_CHUNK_SIZE 	= 1000;
  
  private ConnectionInfo cinfo            			= null;
  private MultipathPolicy writePolicy				= MultipathPolicy.MULTIPATH_POLICY_ROUNDROBIN;

  /**
   * @param out
   * @param cinfo
   * @param fid
   */
  MWrappedOutputStream(ConnectionInfo cinfo)
  {
	  this.cinfo = cinfo;
	    
	    // starts retransmission thread based on policy
	    switch(writePolicy)
	    {
	    	case MULTIPATH_POLICY_RTX_OPT:
	    	{
	    		MultipathWritingPolicy multipathPolicy = new RTTBasedWritingPolicy(cinfo);
	    		cinfo.setMultipathWritingPolicy(multipathPolicy);
	    		break;
	    	}
	    	case MULTIPATH_POLICY_FULL_DUP:
	    	{
	    		MultipathWritingPolicy multipathPolicy = new FullDuplicationWritingPolicy(cinfo);
	    		cinfo.setMultipathWritingPolicy(multipathPolicy);
	    		break;
	    	}
	    	case MULTIPATH_POLICY_ROUNDROBIN:
	    	{
	    		MultipathWritingPolicy multipathPolicy = new RoundRobinWritingPolicy(cinfo);
	    		cinfo.setMultipathWritingPolicy(multipathPolicy);
	    		break;
	    	}
	    	case MULTIPATH_POLICY_BLACKBOX:
	    	{
	    		MultipathWritingPolicy multipathPolicy = new BlackBoxWritingPolicy(cinfo);
	    		cinfo.setMultipathWritingPolicy(multipathPolicy);
	    		break;
	    	}
	    }
  }

  /**
    *
    */
  public void write(byte[] b) throws IOException
  {
    write(b, 0, b.length, DataMessage.DATA_MESG);
  }

  /**
   * @param b
   * @param mgType
   * @throws IOException
   * @throws InterruptedException
   */
  public void write(byte[] b, int mgType) throws IOException
  { // for sending the close message
    write(b, 0, b.length, mgType);
  }

  /**
    *
    */
  public void write(byte[] b, int offset, int length) throws IOException
  { // application calls this function to send data message
    write(b, offset, length, DataMessage.DATA_MESG);
  }

  
  /**
   * @param b
   * @param offset
   * @param length
   * @param MesgType
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void write(byte[] b, int offset, int length, int MesgType) throws IOException
  {
    // FIXME: add checks so that app doesn't passes 0 as size
    if (cinfo == null)
      throw new IOException("Connection must be established before a write");
    if (cinfo.getCloseInOutbuffer() == true)
      throw new IOException("Don't write after closing the socket");
    if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
      throw new IOException(" socket already closed");

    cinfo.setState(ConnectionInfo.READ_WRITE, true);

    if (length != 0)
    {
      {
        cinfo.addOutBuffer(b, offset, length); // first write to outbuffer
        MSocketLogger.getLogger().fine("write " + b[0]);
      }
    }
    else
    {
      if (MesgType == DataMessage.FIN)
      {
        cinfo.setCloseInOutbuffer(true);
      }
    }

    cinfo.setblockingFlag(true);

    try
    {
      MSocketLogger.getLogger().fine("message here length " + length + " dataAckSeq " + cinfo.getDataAckSeq() + "send seq num "
          + cinfo.getDataSendSeq());
      writeInternal(b, offset, length, MesgType);
      cinfo.setState(ConnectionInfo.ALL_READY, true);
      cinfo.setblockingFlag(false);
    }
    catch (IOException e)
    {
      cinfo.setState(ConnectionInfo.ALL_READY, true);
      MSocketLogger.getLogger().fine("IOException blocking starts");

      synchronized (cinfo.getBlockingFlagMonitor())
      {
        while (cinfo.getblockingFlag() && (cinfo.getMSocketState() == MSocketConstants.ACTIVE))
        /*
         * cannot block on socket which other side has explicitly closed
         */
        {
          try
          {
            cinfo.getBlockingFlagMonitor().wait();
          }
          catch (InterruptedException e1)
          {
            e1.printStackTrace();
          }
        }
      }
      if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
      {
        throw new IOException(" socket already closed");
      }

      MSocketLogger.getLogger().fine("IOException blocking ends");
      // assuming rensendIfNeeded sends the data missed here
    }
    // FIXME; need to check with migration scenario
    if (length != 0)
    {
      cinfo.updateDataSendSeq(length);

      // wakeup the background thread to retransmit
      /*synchronized (cinfo.getBackgroundThreadMonitor())
      {
        cinfo.getBackgroundThreadMonitor().notifyAll();
      }*/
      TemporaryTasksES.startTaskWithES(cinfo, TemporaryTasksES.BACKGROUND_RETRANSMIT);
    }
  }

  /**
   * @see java.io.OutputStream#write(int)
   */
  public void write(int b) throws IOException
  {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf);
  }

  public void flush()
  {

  }

  // private methods
  

  /**
   * Function for multipath write over multiple sockets
   * 
   * @param b
   * @param offset
   * @param length
   * @param MesgType
   * @throws IOException
   */
  private void multiPathWrite(byte[] b, int offset, int length, int MesgType) throws IOException
  {
    switch (MesgType)
    {
      case DataMessage.DATA_MESG :
      {
    	cinfo.getMultipathWritingPolicy().writeAccordingToPolicy(b, offset, length, MesgType);
    	
        break;
      }
      case DataMessage.FIN :
      {
    	// to prevent stream desynchronization problem  
    	cinfo.emptyTheWriteQueues();
        boolean FINSuccessful = false;
        while (!FINSuccessful)
        {
          /*
           * randomly choosing the socket to send chunk
           */
          SocketInfo Obj = cinfo.getActiveSocket(cinfo.getMultipathPolicy());
          if (Obj != null)
          {
            while (!Obj.acquireLock())
              ;
            try
            {
              DataMessage dm = new DataMessage(MesgType, cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), 0, 0, null, -1);
              byte[] writebuf = dm.getBytes();

              // exception of wite means that socket is undergoing migration,
              // make it not active, and transfer same data chuk over another
              // available socket.
              // at receiving side, recevier will take care of redundantly
              // received data

              MSocketLogger.getLogger().fine("Using socketID " + Obj.getSocketIdentifer() + " for writing FIN");
              ByteBuffer writeByBuff = ByteBuffer.wrap(writebuf);

              while (writeByBuff.hasRemaining())
              {
                Obj.getSocket().getChannel().write(writeByBuff);
              }
              // FIXME: need to check if it becomes true and exception occurs
              // after that
              FINSuccessful = true;
              Obj.releaseLock();
            }
            catch (IOException ex)
            {
              MSocketLogger.getLogger().fine("Write exception caused on writing FIN");
              Obj.setStatus(false);
              FINSuccessful = false;
              Obj.releaseLock();
            }
          }
          else
          {
            FINSuccessful = false;

            synchronized (cinfo.getSocketMonitor())
            {
              while (cinfo.getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM) == null
                  && (cinfo.getMSocketState() == MSocketConstants.ACTIVE))
              {
                try
                {
                  cinfo.getSocketMonitor().wait();
                }
                catch (InterruptedException e)
                {
                  e.printStackTrace();
                }
              }
            }

            if (cinfo.getMSocketState() == MSocketConstants.CLOSED)
            {
              throw new IOException(" socket already closed");
            }
            // throw exception and block or wait in while loop to check for any
            // available sockets
          }
        }
        break;
      }
    }
  }

  /**
    *
    */
  private void writeInternal(byte[] b, int offset, int length, int MesgType) throws IOException
  {
    {
      // first close message is sent through app writes so that app writes ends
      // before sending this one
      multiPathWrite(b, offset, length, MesgType);
    }
  }

  // commented code, will go after testing
}