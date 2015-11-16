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

/**
 * This class defines the structure of inbuffer storage chunks. It stores the
 * data, start seq num and length of the data.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class InBufferStorageChunk
{
  byte[]     chunkData;
  final long startSeqNum;
  final int  chunkSize;

  InBufferStorageChunk(byte[] data, int offset, long startSeqNum, int chunkSize)
  {
    chunkData = new byte[chunkSize];
    System.arraycopy(data, offset, chunkData, 0, chunkSize);
    this.startSeqNum = startSeqNum;
    this.chunkSize = chunkSize;
  }
}
