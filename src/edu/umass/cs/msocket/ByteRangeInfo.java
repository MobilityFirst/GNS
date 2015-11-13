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
 * This class stores byte ranges of the data and the socket object, on which the
 * data was initially transmitted. Used in defualt multipath data scheduling
 * policy.
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ByteRangeInfo
{
  private long startSeqNum;
  private int  length   = 0;
  private int  socketId = -1;

  ByteRangeInfo(long startSeqNum, int length, int socketId)
  {
    this.startSeqNum = startSeqNum;
    this.length = length;
    this.socketId = socketId;
  }

  public long getStartSeqNum()
  {
    return startSeqNum;
  }

  public int getLength()
  {
    return length;
  }

  public int getSocketId()
  {
    return socketId;
  }
}