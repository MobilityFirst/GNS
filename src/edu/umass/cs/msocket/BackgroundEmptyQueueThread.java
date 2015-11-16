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

import java.util.Vector;

/**
 * This class implements the thread to empty the queue, if no further 
 * write occurs to empty the queue
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class BackgroundEmptyQueueThread  implements Runnable 
{
	
	private ConnectionInfo cinfo				= null;
	private boolean runningStatus				= true;
	
	public BackgroundEmptyQueueThread(ConnectionInfo cinfo)
	{
		this.cinfo = cinfo;
	}
	
	@Override
	public void run()
	{
		//while(runningStatus)
		{
			/*synchronized(cinfo.getEmptyQueueThreadMonitor())
			{
				try {
					cinfo.getEmptyQueueThreadMonitor().wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}*/
			
			while(queuesNotEmpty())
			{
				 cinfo.blockOnOutputStreamSelector();
				 cinfo.setState(ConnectionInfo.READ_WRITE, true);
				 cinfo.attemptToEmptyTheWriteQueues();
		         cinfo.setState(ConnectionInfo.ALL_READY, true);
			}
		}
		
		// work done set it false
		cinfo.setEmptyQueueActive(false);
	}
	
	/**
	  * 
	  * Returns true if queues not empty, false 
	  * otherwise.
	  * @return
	  */
	  private boolean queuesNotEmpty()
	  {
		  Vector<SocketInfo> socketList = new Vector<SocketInfo>();
		  socketList.addAll(cinfo.getAllSocketInfo());
	
		  boolean returnValue = false;
	      
	      for (int i = 0; i < socketList.size(); i++)
	      {
	        SocketInfo Obj = socketList.get(i);
	        if (Obj.getStatus())
	        {
	          if ((Integer) Obj.queueOperations(SocketInfo.QUEUE_SIZE, null) > 0)
	          {
	        	  returnValue = true;
	        	  break;
	          }
	        }
	      }
	      return returnValue;
	  }
	  
	  /*public void stopThread()
	  {
	    runningStatus = false;
	  }*/
}