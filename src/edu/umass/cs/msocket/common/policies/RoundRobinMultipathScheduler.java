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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.umass.cs.msocket.ConnectionInfo;
import edu.umass.cs.msocket.MWrappedOutputStream;
import edu.umass.cs.msocket.logger.MSocketLogger;


public class RoundRobinMultipathScheduler extends MultipathSchedulerInterface
{
	// keeps numBytes transferred on that path,<PathID, NumBytes>map
	private HashMap<Integer, Integer> pathMap 			= null;
	
	// returns path in round robin manner
	private int nextPathIndex				  			= 0;
	
	public RoundRobinMultipathScheduler(ConnectionInfo cinfo)
	{
		this.cinfo = cinfo;
		chunkInfoMap = new HashMap<Integer, ChunkInformation>();
		pathMap = new HashMap<Integer, Integer>();
	}

	@Override
	public void initializeScheduler(int startSeqNum, int numChunks,
			List<Integer> activePathIDs)
	{
		MSocketLogger.getLogger().fine("initializeScheduler(): result startSeqNum " + startSeqNum +
				" numChunks "+numChunks);
		this.startSeqNum = startSeqNum;
		this.numChunks = numChunks;
		this.activePathIDs = activePathIDs;
	}

	@Override
	public List<ChunkInformation> getSchedulingScheme()
	{
		List<ChunkInformation> result = new LinkedList<ChunkInformation>();
		int tempStartSeqNum = this.startSeqNum;
		
		for(int i=0;i<numChunks;i++)
		{
			int nextPathId = activePathIDs.get(nextPathIndex%activePathIDs.size());
			nextPathIndex++;
			
			ChunkInformation chunkInfo = new ChunkInformation(tempStartSeqNum, nextPathId, 0);
			//chunkInfoMap.put(tempStartSeqNum, chunkInfo);
			MSocketLogger.getLogger().fine("getSchedulingScheme(): result tempStartSeqNum "+tempStartSeqNum+
					" favoritePathId "+nextPathId);
			tempStartSeqNum  += MWrappedOutputStream.WRITE_CHUNK_SIZE;
			result.add(chunkInfo);
		}
		return result;
	}

	@Override
	public void informChunkWrite(List<ChunkInformation> chunkWriteList)
	{
		for(int i=0; i< chunkWriteList.size(); i++)
		{
			ChunkInformation chunkInfo = chunkWriteList.get(i);
			//chunkInfoMap.put(chunkInfo.getChunkStartSeqNum(), chunkInfo);
		}
	}

	@Override
	public void informAckArrival(ChunkInformation chunkAckList)
	{
		//remove chunk from chunk map
		//chunkInfoMap.remove(chunkAckList.getChunkStartSeqNum());
		
		int pathID = chunkAckList.getPathID();
		
		if( pathMap.containsKey(pathID) )
		{
			int bytesRecv = pathMap.get(pathID);
			bytesRecv += 1;
			pathMap.put(pathID, bytesRecv);
		}
		else
		{
			// 1 chunk recvd
			pathMap.put(pathID, 1);
		}
	}
	
}