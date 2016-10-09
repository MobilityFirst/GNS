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
import java.util.Collection;
import java.util.Vector;


import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.DataMessage;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.logger.MSocketLogger;

/**
 * 
 * This class defines a FullDuplicationWritingPolicy
 * This policy writes a chunk on both the paths. In one it is written 
 * in forward order and on the other path it is written in backwards order.
 * Currently this policy will use only two paths
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class FullDuplicationWritingPolicy extends MultipathWritingPolicy 
{	
	public FullDuplicationWritingPolicy(ConnectionInfo cinfo)
	  {
	    this.cinfo = cinfo;
	  }
	
	@Override
	public void writeAccordingToPolicy(byte[] b, int offset, int length,
			int MesgType) throws IOException {
		
		int currposPath1 				= 0;
	    int remainingPath1 				= length;
	    int tempDataSendSeqNumPath1		= cinfo.getDataSendSeq();
	    
	    
	    int currposPath2 				= 0;
	    int remainingPath2 				= length;
	    int tempDataSendSeqNumPath2 	= cinfo.getDataSendSeq() + length;
	    

	    while (true)
	    {
	      // blocks on selector, until the channel is
	      // available to write
	      cinfo.blockOnOutputStreamSelector();
	      // reads input stream for ACKs an stores data in input buffer
	      try
	      {
	        cinfo.multiSocketRead();
	      }
	      catch (IOException ex)
	      {
	        ex.printStackTrace();
	      }
	      
	      Vector<SocketInfo> socketMapValues = new Vector<SocketInfo>();
	      socketMapValues.addAll(cinfo.getAllSocketInfo());
	      
	      SocketInfo sockObj1 = null;
	      SocketInfo sockObj2 = null;
	      
	      if( socketMapValues.size() > 0 )
	      {
	    	  sockObj1 = socketMapValues.get(0);
	      }
	      if( socketMapValues.size() > 1 )
	      {
	    	  sockObj2 = socketMapValues.get(1);
	      }
	      
	      
	      if( (sockObj1 != null) && (remainingPath1>0) )
	      {
		        while (!sockObj1.acquireLock());
		        
		        int tobesent = 0;
		        if (remainingPath1 < MWrappedOutputStream.WRITE_CHUNK_SIZE)
		        {
		          tobesent = remainingPath1;
		        }
		        else
		        {
		          tobesent = MWrappedOutputStream.WRITE_CHUNK_SIZE;
		        }

		        try
		        {
		          // System.arraycopy(b, offset + currpos, buf, 0, tobesent);
		          int arrayCopyOffset = offset + currposPath1;
		          DataMessage dm = new DataMessage(MesgType, tempDataSendSeqNumPath1, cinfo.getDataAckSeq(), tobesent, 0, b,
		              arrayCopyOffset);
		          byte[] writebuf = dm.getBytes();

		          // exception of write means that socket is undergoing migration,
		          // make it not active, and transfer same data chunk over another
		          // available socket.
		          // at receiving side, receiver will take care of redundantly
		          // received data

		          
		            if ((Integer) sockObj1.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
		            {
		              cinfo.attemptSocketWrite(sockObj1);
		              sockObj1.releaseLock();
		            }
		            else
		            {
		              //System.out.println("Writing "+tempDataSendSeqNumPath1+" on socket 1");
		              sockObj1.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
		              
		              cinfo.attemptSocketWrite(sockObj1);
			          
			          sockObj1.updateSentBytes(tobesent);
			          currposPath1 += tobesent;
			          remainingPath1 -= tobesent;
			          tempDataSendSeqNumPath1 += tobesent;
			          sockObj1.releaseLock();
		            }
		        }
		        catch (IOException ex)
		        {
		          MSocketLogger.getLogger().fine("Write exception caused");
		          sockObj1.setStatus(false);
		          sockObj1.setneedToReqeustACK(true);

		          sockObj1.updateSentBytes(tobesent);
		          currposPath1 += tobesent;
		          remainingPath1 -= tobesent;
		          tempDataSendSeqNumPath1 += tobesent;
		          sockObj1.releaseLock();
		        }
	      }
	      
	      if( (sockObj2 != null) && (remainingPath2>0) )
	      {
		        while (!sockObj2.acquireLock());
		        
		        int tobesent = 0;
		        if (remainingPath2 < MWrappedOutputStream.WRITE_CHUNK_SIZE)
		        {
		          tobesent = remainingPath2;
		        }
		        else
		        {
		          tobesent = MWrappedOutputStream.WRITE_CHUNK_SIZE;
		        }

		        try
		        {
		          // System.arraycopy(b, offset + currpos, buf, 0, tobesent);
		          int arrayCopyOffset = offset + length - currposPath2 - tobesent;
		          DataMessage dm = new DataMessage(MesgType, tempDataSendSeqNumPath2-tobesent, cinfo.getDataAckSeq(), tobesent, 0, b,
		              arrayCopyOffset);
		          byte[] writebuf = dm.getBytes();

		          // exception of write means that socket is undergoing migration,
		          // make it not active, and transfer same data chunk over another
		          // available socket.
		          // at receiving side, receiver will take care of redundantly
		          // received data

		          
		            if ((Integer) sockObj2.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
		            {
		              cinfo.attemptSocketWrite(sockObj2);
		              sockObj2.releaseLock();
		            }
		            else
		            {
		              //System.out.println("Writing "+tempDataSendSeqNumPath2+" on socket 2");
		              sockObj2.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
		              
		              cinfo.attemptSocketWrite(sockObj2);
			          
			          sockObj2.updateSentBytes(tobesent);
			          currposPath2 += tobesent;
			          remainingPath2 -= tobesent;
			          tempDataSendSeqNumPath2 -= tobesent;
			          sockObj2.releaseLock();
		            }
		        }
		        catch (IOException ex)
		        {
		          MSocketLogger.getLogger().fine("Write exception caused");
		          sockObj2.setStatus(false);
		          sockObj2.setneedToReqeustACK(true);

		          sockObj2.updateSentBytes(tobesent);
		          currposPath2 += tobesent;
		          remainingPath2 -= tobesent;
		          tempDataSendSeqNumPath2 -= tobesent;
		          sockObj2.releaseLock();
		        }
	      }
	      
	      if( (remainingPath1 <= 0) && (remainingPath2 <= 0) )
	      {
	    	  break;
	      }
	    }

      Vector<SocketInfo> socketList = new Vector<SocketInfo>();
      socketList.addAll( (Collection<? extends SocketInfo>) cinfo.getAllSocketInfo() );
      String print = "";
      for (int i = 0; i < socketList.size(); i++)
      {
        SocketInfo Obj = socketList.get(i);
        if ( Obj.getStatus() )
        {
          print += "socketID " + Obj.getSocketIdentifer() + " SentBytes " + Obj.getSentBytes() + " "
              + " RecvdBytesOtherSide " + Obj.getRecvdBytesOtherSide() + " ";
        }
      }
      
      // need to empty the write queues here, can't return
      // before that, otherwise it would desynchronize the output stream
      cinfo.emptyTheWriteQueues();
	}

	protected SocketInfo getNextSocketToWrite() throws IOException {
		
		return null;
	}
	
}