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
import java.util.List;

import edu.umass.cs.msocket.ConnectionInfo;

/**
 * 
 * This class defines the interface of the multipath policy scheduler
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public abstract class MultipathSchedulerInterface 
{	
	protected ConnectionInfo cinfo								= null;
	protected int startSeqNum									= 0;
	protected int numChunks										= 0;
	protected List<Integer> activePathIDs						= null;
	protected HashMap<Integer, ChunkInformation> chunkInfoMap	= null;	
	
	/**
	 * Initializes the scheduler with startSeqNum, numChunks and active path ids
	 * Each chunk is 1000 bytes, as defined in MWrappedOutputStream.WRITE_CHUNK_SIZE
	 * 
	 * @param startSeqNum
	 * @param numChunks
	 * @param activePathIDs
	 */
	public abstract void initializeScheduler(int startSeqNum, int numChunks, List<Integer> activePathIDs);
	
	
	/**
	 * returns the pathID for each chunk
	 *
	 */
	public abstract List<ChunkInformation> getSchedulingScheme();
	
	/**
	 * 
	 * Informs the scheduler which chunk got written to which path
	 * @param chunkInfo
	 */
	public abstract void informChunkWrite(List<ChunkInformation> chunkWriteList);
	
	/**
	 * Informs the scheduler about the bytes received on both the paths
	 * 
	 * @param chunkInfo
	 */
	public abstract void informAckArrival(ChunkInformation chunkAckList);
}