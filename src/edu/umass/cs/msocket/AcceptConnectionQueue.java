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

import java.util.LinkedList;
import java.util.Queue;

/**
 * Class implements a queue for the accepted MSockets in the MServerSocket.
 * Executor service accepts the connect and puts those in this queue and accept
 * methods dequeues the accepted socket to be returned to the application
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class AcceptConnectionQueue
{
  private Queue<Object>   AcceptQueue;
  public static final int GET_SIZE = 1;
  public static final int PUT      = 2;
  public static final int GET      = 3;

  public AcceptConnectionQueue()
  {
    AcceptQueue = new LinkedList<Object>();
  }

  /**
   * Synchronized method to GET, PUT, SIZE on the queue
   * 
   * @param Type
   * @param ReadySocket
   * @return
   */
  public synchronized Object getFromQueue(int Type, Object ReadySocket)
  {
    switch (Type)
    {
      case GET_SIZE :
      {
        return AcceptQueue.size();
      }
      case PUT :
      {
        AcceptQueue.add(ReadySocket);
        break;
      }
      case GET :
      {
        return AcceptQueue.poll();
      }
    }
    return null;
  }
}