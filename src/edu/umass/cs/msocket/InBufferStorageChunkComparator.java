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

package edu.umass.cs.msocket;

import java.util.Comparator;

/**
 * Inbuffer chunk comparator, to store the inbuffer
 * chunks in sorted order in inbuffer as priority queue.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class InBufferStorageChunkComparator implements Comparator<InBufferStorageChunk> {
	    @Override
	    public int compare(InBufferStorageChunk x, InBufferStorageChunk y)
	    {
	        // Assume neither string is null. Real code should
	        // probably be more robust
	        if ( x.startSeqNum < y.startSeqNum )
	        {
	            return -1;
	        }
	        if (x.startSeqNum > y.startSeqNum )
	        {
	            return 1;
	        }
	        return 0;
	    }
}