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

package edu.umass.cs.msocket.common.policies;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Vector;

import edu.umass.cs.msocket.ByteRangeInfo;
import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.DataMessage;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.logger.MSocketLogger;


/**
 * 
 * This class defines the parent class for different
 * multipath write policies
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class MultipathWritingPolicy {
	
	protected ConnectionInfo cinfo			= null;
	
	
	/**
	 * child class implements the write call according to the
	 * policy.
	 * @throws IOException 
	 */
	public abstract void writeAccordingToPolicy(byte[] b, int offset, int length, int MesgType) throws IOException;
	
	
	  /**
	   * @param tempDataSendSeqNum
	   * @param Obj
	   * @throws IOException
	   */
	  protected void handleMigrationInMultiPath(SocketInfo Obj) throws IOException
	  {

	    MSocketLogger.getLogger().fine("handleMigrationInMultiPath called");
	    // if queue size is > 0 then it means that there is a non-blocking
	    // write pending and it should be sent first, instead of migration data
	    if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
	    {
	      attemptSocketWrite(Obj);
	      return;
	    }

	    MSocketLogger.getLogger().fine("HandleMigrationInMultiPath SocektId " + Obj.getSocketIdentifer());

	    cinfo.multiSocketRead();
	    int dataAck = (int) cinfo.getDataBaseSeq();
	    MSocketLogger.getLogger().fine("DataAck from other side " + dataAck);
	    Obj.byteInfoVectorOperations(SocketInfo.QUEUE_REMOVE, dataAck, -1);

	    @SuppressWarnings("unchecked")
	    Vector<ByteRangeInfo> byteVect = (Vector<ByteRangeInfo>) Obj.byteInfoVectorOperations(SocketInfo.QUEUE_GET, -1, -1);

	    for (int i = 0; i < byteVect.size(); i++)
	    {
	      ByteRangeInfo currByteR = byteVect.get(i);

	      if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
	      {
	        // setting the point to start from next time
	        Obj.setHandleMigSeqNum(currByteR.getStartSeqNum());
	        attemptSocketWrite(Obj);
	        return;
	      }

	      cinfo.multiSocketRead();
	      dataAck = (int) cinfo.getDataBaseSeq();

	      // already acknowledged, no need to send again
	      if (dataAck > (currByteR.getStartSeqNum() + currByteR.getLength()))
	      {
	        continue;
	      }

	      // if already sent
	      if ((currByteR.getStartSeqNum() + currByteR.getLength()) < Obj.getHandleMigSeqNum())
	      {
	        continue;
	      }

	      byte[] buf = cinfo.getDataFromOutBuffer(currByteR.getStartSeqNum(),
	          currByteR.getStartSeqNum() + currByteR.getLength());

	      int arrayCopyOffset = 0;
	      DataMessage dm = new DataMessage(DataMessage.DATA_MESG, (int) currByteR.getStartSeqNum(), cinfo.getDataAckSeq(),
	          buf.length, 0, buf, arrayCopyOffset);
	      byte[] writebuf = dm.getBytes();

	      Obj.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
	      attemptSocketWrite(Obj);

	    }

	    Obj.setneedToReqeustACK(false);
	    MSocketLogger.getLogger().fine("HandleMigrationInMultiPath Complete");
	  }
	  
	  
	  protected void attemptSocketWrite(SocketInfo Obj) throws IOException
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
	      MSocketLogger.getLogger().fine("gotWritten " + gotWritten + " buf length " + writebuf.length + " SocketID " + Obj.getSocketIdentifer());
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

	    Obj.getDataChannel().configureBlocking(false);
	    if (gotWritten > 0)
	      MSocketLogger.getLogger().fine("Using socketID " + Obj.getSocketIdentifer() + "Remote IP " + Obj.getSocket().getInetAddress()
	          + "for writing " + " time taken " + (endTime - startTime));
	  }
}