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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TemporaryTasksES
{
	public static final int							BACKGROUND_RETRANSMIT	= 1;
	public static final int							EMPTY_QUEUE				= 2;
	
	
	private static ExecutorService     				pool;
    
    // the static singleton object
    private static TemporaryTasksES					temporaryTaskObj        = null;
    
    
    /**
     * 
     * TODO: startTaskWithES definition.
     * 
     * @param cinfo
     * @param operType
     */
    public synchronized static void startTaskWithES(ConnectionInfo cinfo, int operType)
    {
      createSingleton();
      
      switch(operType)
      {
	      case BACKGROUND_RETRANSMIT:
	      {
	    	  // with setting and unsettting like this,
	    	  // the last chunk might not get retransmitted, if
	    	  // there are no further transmissions
	    	  // if not active
	    	  if(!cinfo.getBackgroundThreadActive())
	    	  {
	    		  // set it to active
	    		  cinfo.setBackgroundThreadActive(true);
	    		  pool.execute(new BackgroundWritingThread(cinfo));
	    	  }
	    	  break;
	      }
	      case EMPTY_QUEUE:
	      {
	    	  if(!cinfo.getEmptyQueueActive())
	    	  {
	    		  // set it to active
	    		  cinfo.setEmptyQueueActive(true);
	    		  pool.execute(new BackgroundEmptyQueueThread(cinfo));
	    	  }
	    	  break;
	      }
      }
    }
    
    public synchronized static void shutdownES()
    {
    	if(pool!=null)
    		pool.shutdown();
    }
    
    /**
     * private constructor
     */
    private TemporaryTasksES()
    {
    	pool = Executors.newCachedThreadPool();
    }

    /**
     * Checks if the singleton object is created or not, if not it creates the
     * object and then the object is returned.
     * 
     * @return the singleton object
     */
    private static void createSingleton()
    {
      if (temporaryTaskObj == null)
      {
    	temporaryTaskObj = new TemporaryTasksES();
      }
    }
    
}