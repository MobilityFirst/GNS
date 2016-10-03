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
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.DataMessage;
import edu.umass.cs.msocket.MSocketConstants;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.MultipathPolicy;
import edu.umass.cs.msocket.SocketInfo;
import edu.umass.cs.msocket.logger.MSocketLogger;

public class BlackBoxWritingPolicy extends MultipathWritingPolicy 
{
	private MultipathSchedulerInterface writingInterface				= null;

	public BlackBoxWritingPolicy(ConnectionInfo cinfo)
	{
		this.cinfo = cinfo;
		writingInterface = new RoundRobinMultipathScheduler(cinfo);
		//cinfo.startRetransmissionThread();
		//cinfo.startEmptyQueueThread();
	}
	
	
	@Override
	public void writeAccordingToPolicy(byte[] b, int offset, int length,
			int MesgType) throws IOException 
	{
		double lenDouble = length;
		int numChunks = (int)Math.ceil(lenDouble/MWrappedOutputStream.WRITE_CHUNK_SIZE);
		
		//int begSeqNum = cinfo.getDataSendSeq();
		
		writingInterface.initializeScheduler(cinfo.getDataSendSeq(), numChunks, getActivePathIDs());
		
		List<ChunkInformation> chunkInfoList = writingInterface.getSchedulingScheme();
		
		List<ChunkInformation> actualChunkWrite = new LinkedList<ChunkInformation>();
		
		int remaining = length;
		
		for(int i=0; i<chunkInfoList.size(); )
		{
			int actualPathID = -1;
			ChunkInformation chunkInfo  = chunkInfoList.get(i);
			
			int currChunkLength;
			if(remaining < MWrappedOutputStream.WRITE_CHUNK_SIZE)
			{
				currChunkLength = remaining;
			}
			else
			{
				currChunkLength = MWrappedOutputStream.WRITE_CHUNK_SIZE;
			}
			
			int actualOffset = offset + length - remaining;
			boolean success = writeChunk(cinfo.getSocketInfo(chunkInfo.getPathID()), chunkInfo.getChunkStartSeqNum(), 
					b, actualOffset, currChunkLength);
			
			
			// unable to follow black box advice
			if( !success )
			{
				//try writing over any path
				Vector<SocketInfo> socketMapValues = new Vector<SocketInfo>();
			    socketMapValues.addAll(cinfo.getAllSocketInfo());
			    
		        int j = 0;
		        while ( j < socketMapValues.size() )
		        {
		          SocketInfo value = socketMapValues.get(j);
		          if( value.getStatus() )
		          {
		        	  success = writeChunk(value, chunkInfo.getChunkStartSeqNum(), 
		  					b, actualOffset, currChunkLength);
		        	  // written successfully
		        	  if(success)
		        	  {
		        		  actualPathID = value.getSocketIdentifer();
		        		  break;
		        	  }
		          }
		          j++;
		        }
			} 
			else
			{
				actualPathID = chunkInfo.getPathID();
			}
			
			if(!success)
	        {
	        	// if couldn't write then block on the 
	        	// outputstream selector.
	        	cinfo.blockOnOutputStreamSelector();
	        } else
	        {
	        	ChunkInformation actualChunkInfo = new  ChunkInformation(chunkInfo.getChunkStartSeqNum(), actualPathID, 0);
	        	actualChunkWrite.add(actualChunkInfo);
	        	remaining = remaining - currChunkLength;
	        	i++;
	        }
		}
		
		writingInterface.informChunkWrite(actualChunkWrite);
	}

	protected SocketInfo getNextSocketToWrite() throws IOException 
	{
		return null;
	}
	
	private List<Integer> getActivePathIDs()
	{
		List<Integer> pathIDList = new LinkedList<Integer>();
		
		Vector<SocketInfo> socketMapValues = new Vector<SocketInfo>();
	    socketMapValues.addAll(cinfo.getAllSocketInfo());
	    
        int i = 0;
        while (i < socketMapValues.size())
        {
          SocketInfo value = socketMapValues.get(i);
          if(value.getStatus())
          {
        	  pathIDList.add( value.getSocketIdentifer() );
          }
          i++;
        }
		return pathIDList;
	}
	
	private boolean writeChunk(SocketInfo sockObj, int startSeqNum, byte[] bytebuf, int bufferOffset, int chunkLength) throws IOException
	{
		if (sockObj != null)
	      {
	        while (!sockObj.acquireLock());
	        
	        sockObj.byteInfoVectorOperations(SocketInfo.QUEUE_REMOVE, cinfo.getDataBaseSeq(), -1);
	        int tobesent = chunkLength;
	        
	        try
	        {
	          if (sockObj.getneedToReqeustACK() )
	          {
	            handleMigrationInMultiPath(sockObj);
	            //sockObj.releaseLock();
	            //continue;
	          }
	          
	          DataMessage dm = new DataMessage(DataMessage.DATA_MESG, startSeqNum, cinfo.getDataAckSeq(), tobesent, 0, bytebuf,
	        		  bufferOffset);
	          byte[] writebuf = dm.getBytes();

	          // exception of write means that socket is undergoing migration,
	          // make it not active, and transfer same data chunk over another
	          // available socket.
	          // at receiving side, receiver will take care of redundantly
	          // received data

	          
		        if ((Integer) sockObj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
		        {
		          cinfo.attemptSocketWrite(sockObj);
		          sockObj.releaseLock();
		          return false;
		        }
		        else
		        {
		        	sockObj.queueOperations(SocketInfo.QUEUE_PUT, writebuf);
		        	sockObj.byteInfoVectorOperations(SocketInfo.QUEUE_PUT, startSeqNum, tobesent);
		        }
		
		        cinfo.attemptSocketWrite(sockObj);
		        //if (cinfo.getServerOrClient() == MSocketConstants.CLIENT)
		        {
		          MSocketLogger.getLogger().fine("Using socketID " + sockObj.getSocketIdentifer() + "Remote IP " + sockObj.getSocket().getInetAddress()
		              + "for writing " + "" + "tempDataSendSeqNum " + startSeqNum);
		        }
	          
		      sockObj.updateSentBytes(tobesent);
	          sockObj.releaseLock();
	        }
	        catch (IOException ex)
	        {
	          MSocketLogger.getLogger().fine("Write exception caused");
	          sockObj.setStatus(false);
	          sockObj.setneedToReqeustACK(true);

	          sockObj.updateSentBytes(tobesent);
	          sockObj.releaseLock();
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
		
		return true;
	}
	
	/**
	 * 
	 * Informs the ack arrival to the black box.
	 */
	public void informAckArrival(ChunkInformation chunkAckList)
	{
		writingInterface.informAckArrival(chunkAckList);
	}
	
}