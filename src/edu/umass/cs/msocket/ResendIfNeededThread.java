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
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

/**
 * This class implements a thread that resends the data, in orderly manner,
 * after the migrations.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ResendIfNeededThread implements Runnable
{
  private ConnectionInfo cinfo;
  private static Logger  log = Logger.getLogger(ResendIfNeededThread.class.getName());

  ResendIfNeededThread(ConnectionInfo cinfo)
  {
    this.cinfo = cinfo;
  }

  public void run()
  {
    try
    {
      log.trace("ResendIfNeededThread trying to get READ_WRITE");
      cinfo.setState(ConnectionInfo.READ_WRITE, true);
      resendIfNeeded(cinfo);
      // FIXME: may be better method
      cinfo.setblockingFlag(false);

      cinfo.setState(ConnectionInfo.ALL_READY, true);
      log.trace("Set server state to ALL_READY");
    }
    catch (IOException ex)
    {
      log.trace("Succesive migration: exception during migration");
      cinfo.setState(ConnectionInfo.ALL_READY, true);
    }
  }

  private void resendIfNeeded(ConnectionInfo cinfo) throws IOException
  {
    log.trace("resendIfNeeded called");
    if (cinfo.getDataBaseSeq() < cinfo.getDataSendSeq())
    {
      // need to resend
      log.trace("fetching resend data from  out buffer");
      handleMigrationInMultiPath(cinfo.getDataSendSeq(), cinfo.getActiveSocket(cinfo.getMultipathPolicy()));
    }

    // FIXME: currently close will not work in Migrations
    // FIXME: need to check if FIN and ACK can be sent from any state on
    // migraton or specific states. In other states on receving that message it
    // ignores it
    
      if( cinfo.getCloseInOutbuffer()) 
    	  // close resent again on migration as no ACK has been recevied till now 
    	  { 
    	  	  log.debug("sending FIN again");
	    	  sendMesgAgain(DataMessage.FIN);
    	  }
      
      if( cinfo.getACKInOutbuffer()) 
    	  // close resent again on migration as no ACK has been recevied till now 
    	  { 
    	  log.debug("sending close ACK again");
    	  	sendMesgAgain(DataMessage.ACK);
    	  } 
  }

  /**
   * resends data if needed
   * 
   * @param tempDataSendSeqNum
   * @param Obj
   * @throws IOException
   */
  private void handleMigrationInMultiPath(int tempDataSendSeqNum, SocketInfo Obj) throws IOException
  {
	  cinfo.emptyTheWriteQueues();
    log.trace("HandleMigrationInMultiPath End Seq Num" + tempDataSendSeqNum + " SocektId " + Obj.getSocketIdentifer());
    cinfo.multiSocketRead();
    int dataAck = (int) cinfo.getDataBaseSeq();
    log.trace("DataAck from other side " + dataAck);

    if (tempDataSendSeqNum > dataAck)
    {
      byte[] buf = cinfo.getDataFromOutBuffer(dataAck, tempDataSendSeqNum);

      // FIXME: change it to chunks
      int arrayCopyOffset =0;
      DataMessage dm = new DataMessage(DataMessage.DATA_MESG, dataAck, cinfo.getDataAckSeq(), buf.length, 0, buf, arrayCopyOffset);
      byte[] writebuf = dm.getBytes();

      // exception of wite means that socket is undergoing migration, make it
      // not active, and transfer same data chunk over another available socket.
      // at receiving side, recevier will take care of redundantly received data
      ByteBuffer writeByBuff = ByteBuffer.wrap(writebuf);
      while (writeByBuff.hasRemaining())
      {
        Obj.getSocket().getChannel().write(writeByBuff);
      }

      Obj.updateSentBytes(buf.length);
    }
    Obj.setneedToReqeustACK(false);
  }
  
  private void sendMesgAgain(int mesgType)
  {
	  cinfo.emptyTheWriteQueues();
	  
	  SocketInfo Obj = cinfo.getActiveSocket(cinfo.getMultipathPolicy());
      if (Obj != null)
      {
        while (!Obj.acquireLock());
        try
        {
          DataMessage dm = new DataMessage(mesgType, cinfo.getDataSendSeq(), cinfo.getDataAckSeq(), 0, 0, null, -1);
          byte[] writebuf = dm.getBytes();

          log.trace("Using socketID " + Obj.getSocketIdentifer() + " for writing FIN");
          ByteBuffer writeByBuff = ByteBuffer.wrap(writebuf);

          while (writeByBuff.hasRemaining())
          {
            Obj.getSocket().getChannel().write(writeByBuff);
          }
          Obj.releaseLock();
        }
        catch (IOException ex)
        {
          log.trace("Write exception caused on writing FIN");
          Obj.setStatus(false);
          Obj.releaseLock();
        }
      }
  }

  // commented code
}