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


public class ChunkInformation {
	
	// contains the chunk start seq num, 
	// end seq num is defined by MWrappedOutputStream.WRITE_CHUNK_SIZE.
	private final int chunkStartSeqNum;
	
	private final int pathID;
	
	private final long bytesRecvd;
	
	public ChunkInformation(int chunkStartSeqNum, int pathID, long bytesRecvd)
	{
		this.chunkStartSeqNum = chunkStartSeqNum;
		this.pathID = pathID;
		this.bytesRecvd = bytesRecvd;
	}
	
	public int getChunkStartSeqNum()
	{
		return chunkStartSeqNum;
	}

	public int getPathID()
	{
		return pathID;
	}
	
	public long getBytesRecvd()
	{
		return this.bytesRecvd;
	}
	
	/*public void setBytesRecvd(long bytesRecvd)
	{
		this.bytesRecvd = bytesRecvd;
	}*/
}