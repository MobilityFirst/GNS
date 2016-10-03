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
import edu.umass.cs.msocket.MSocketConstants;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.MultipathPolicy;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.logger.MSocketLogger;

public class RoundRobinWritingPolicy extends MultipathWritingPolicy 
{	
	private int                      defaultPolicyInterfaceNum  = 0;
	
	public RoundRobinWritingPolicy(ConnectionInfo cinfo)
	{
	    this.cinfo = cinfo;
	    //cinfo.startRetransmissionThread();
	    //cinfo.startEmptyQueueThread();
	}
	
	@Override
	public void writeAccordingToPolicy(byte[] b, int offset, int length, int MesgType) throws IOException
	{
	    int currpos = 0;
	    int remaining = length;
	    int tempDataSendSeqNum = cinfo.getDataSendSeq();

	    //MultipathPolicy writePolicy = MultipathPolicy.MULTIPATH_POLICY_RTX_OPT;

	    while (currpos < length)
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

	      SocketInfo Obj = getNextSocketToWrite(); 				// randomly
	                                                            // choosing the
	                                                            // socket to send
	                                                            // chunk
	      if (Obj != null)
	      {
	        while (!Obj.acquireLock());
	        Obj.byteInfoVectorOperations(SocketInfo.QUEUE_REMOVE, cinfo.getDataBaseSeq(), -1);
	        int tobesent = 0;
	        if (remaining < MWrappedOutputStream.WRITE_CHUNK_SIZE)
	        {
	          tobesent = remaining;
	        }
	        else
	        {
	          tobesent = MWrappedOutputStream.WRITE_CHUNK_SIZE;
	        }

	        try
	        {
	          if (Obj.getneedToReqeustACK())
	          {
	            handleMigrationInMultiPath(Obj);
	            Obj.releaseLock();
	            continue;
	          }

	          // System.arraycopy(b, offset + currpos, buf, 0, tobesent);
	          int arrayCopyOffset = offset + currpos;
	          DataMessage dm = new DataMessage(MesgType, tempDataSendSeqNum, cinfo.getDataAckSeq(), tobesent, 0, b,
	              arrayCopyOffset);
	          byte[] writebuf = dm.getBytes();

	          // exception of write means that socket is undergoing migration,
	          // make it not active, and transfer same data chunk over another
	          // available socket.
	          // at receiving side, receiver will take care of redundantly
	          // received data

	          
		        if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
		        {
		          cinfo.attemptSocketWrite(Obj);
		          Obj.releaseLock();
		          continue;
		        }
		        else
		        {
		          Obj.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
		          Obj.byteInfoVectorOperations(SocketInfo.QUEUE_PUT, tempDataSendSeqNum, tobesent);
		        }
		
		        cinfo.attemptSocketWrite(Obj);
		        //if (cinfo.getServerOrClient() == MSocketConstants.CLIENT)
		        {
		          MSocketLogger.getLogger().fine("Using socketID " + Obj.getSocketIdentifer() + "Remote IP " + Obj.getSocket().getInetAddress()
		              + "for writing " + "" + "tempDataSendSeqNum " + tempDataSendSeqNum);
		        }
	          
	          Obj.updateSentBytes(tobesent);
	          currpos += tobesent;
	          remaining -= tobesent;
	          tempDataSendSeqNum += tobesent;
	          Obj.releaseLock();

	        }
	        catch (IOException ex)
	        {
	          MSocketLogger.getLogger().fine("Write exception caused");
	          Obj.setStatus(false);
	          Obj.setneedToReqeustACK(true);

	          Obj.updateSentBytes(tobesent);
	          currpos += tobesent;
	          remaining -= tobesent;
	          tempDataSendSeqNum += tobesent;
	          Obj.releaseLock();
	        }
	      }
	      else
	      {
	        // throw exception and block or wait in while loop to check for any
	        // available sockets
	        synchronized (cinfo.getSocketMonitor())
	        {
	          while ((cinfo.getActiveSocket(MultipathPolicy.MULTIPATH_POLICY_RANDOM) == null)
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
	      }
	      
	    }
	    
		Vector<SocketInfo> socketList = new Vector<SocketInfo>();
		socketList.addAll((Collection<? extends SocketInfo>) cinfo.getAllSocketInfo());
		String print = "";
		for (int i = 0; i < socketList.size(); i++)
		{
			SocketInfo Obj = socketList.get(i);
			if (Obj.getStatus())
			{
				print += "socketID " + Obj.getSocketIdentifer() + " SentBytes " + Obj.getSentBytes() + " "
				+ " RecvdBytesOtherSide " + Obj.getRecvdBytesOtherSide() + " ";
			}
		}
		  
		// MSocketLogger.getLogger().fine(print);
		// need to empty the write queues here, can't return
		// before that, otherwise it would desynchronize the output stream
		// long emptyQueueStartTime = System.currentTimeMillis();
		// cinfo.emptyTheWriteQueues();
		// long emptyQueueEndTime = System.currentTimeMillis();
		// System.out.println( "Write empty queue time "+(emptyQueueEndTime-emptyQueueStartTime) );
	}
	
	private Vector<SocketInfo> removeClosingFPsFromList()
	{
		Vector<SocketInfo> socketMapValues = new Vector<SocketInfo>();
	    socketMapValues.addAll(cinfo.getAllSocketInfo());
	    
	    Vector<SocketInfo> closingRemoved = new Vector<SocketInfo>();
	    
		for (int i=0;i<socketMapValues.size();i++)
		{
			SocketInfo socketObj = socketMapValues.get(i);
			// if not closing
			if(!socketObj.getClosing())
			{
				closingRemoved.add(socketObj);
			}
		}
		return closingRemoved;
	}

	protected SocketInfo getNextSocketToWrite() throws IOException 
	{
		Vector<SocketInfo> socketMapValues = removeClosingFPsFromList();
	    
		int i = 0;
        SocketInfo value = null;
        SocketInfo retvalue = null;
        int nummaxrun = 0;

        i = 0;
        while (i < socketMapValues.size())
        {
        	
          value = socketMapValues.get(i);

          if (value.getStatus()) // true means active
          {
            if (((Integer) value.queueOperations(SocketInfo.QUEUE_SIZE, null) == 0))
            {
            }
            else
            {
              while (!value.acquireLock());
              cinfo.attemptSocketWrite(value);
              value.releaseLock();
            }
          }
          i++;
        }

        do
        {
          i = 0;
          while (i < socketMapValues.size())
          {
            value = socketMapValues.get(i);

            if (value.getStatus()) // true means active
            {
              if (((Integer) value.queueOperations(SocketInfo.QUEUE_SIZE, null) == 0))
              {
                if ((i >= defaultPolicyInterfaceNum))
                {
                  retvalue = value;
                  break;
                }
                else if (nummaxrun == 1)
                { // rollover case
                  retvalue = value;
                  break;
                }
              }
            }
            i++;
          }
          nummaxrun++;
        }
        while ((retvalue == null) && (nummaxrun < 2)); // to rollover for
        // the first one

        if (retvalue == null)
        { // all paths have
          // choked send buffers
          nummaxrun = 0;
          do
          {
            i = 0;
            while (i < socketMapValues.size())
            {
              value = socketMapValues.get(i);

              if (value.getStatus()) // true means active
              {
                if (nummaxrun == 1)
                {
                  retvalue = value;
                  break;
                }
                else if ((i >= defaultPolicyInterfaceNum))
                {
                  retvalue = value;
                  break;
                }
              }
              i++;
            }
            nummaxrun++;
          }
          while ((retvalue == null) && (nummaxrun < 2)); // to rollover
          // for the first one
        }

        int Size = socketMapValues.size();
        defaultPolicyInterfaceNum++;
        defaultPolicyInterfaceNum = defaultPolicyInterfaceNum % Size;
        
        return retvalue;
	}
}