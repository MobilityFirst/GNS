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

package edu.umass.cs.msocket.proxy;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.gns.DefaultGNSClient;

/**
 * This class defines a TimerKeepalive
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class TimerKeepalive extends Thread
{
  private GuidEntry           guid;
  private int                 publishFrequency;
  private boolean             killed = false;
  private static final Logger logger = Logger.getLogger("KeepAliveTimer");

  /**
   * Creates a new <code>GnsTimerKeepalive</code> object
   * 
   * @param gnsCredentials
   * @param myGuid
   * @param publishFrequency
   * @throws Exception if a GNS error occurs
   */
  public TimerKeepalive(GuidEntry myGuid, int publishFrequency) throws Exception
  {
    this.guid = myGuid;
    this.publishFrequency = publishFrequency;
    logger.fine("Publishing start time");
    final long now = System.currentTimeMillis();
    
    DefaultGNSClient.getGnsClient().execute
    	(GNSCommand.fieldReplaceOrCreateList(myGuid.getGuid(), Constants.START_TIME, 
    			new JSONArray().put(now), myGuid));
    
    DefaultGNSClient.getGnsClient().execute(GNSCommand.aclAdd
    		(AclAccessType.READ_WHITELIST, myGuid, Constants.START_TIME, null));
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.fieldReplaceOrCreateList(myGuid.getGuid(), Constants.TIME_REFRESH_INTERVAL,
            new JSONArray().put(publishFrequency), myGuid) );
    		
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.aclAdd
    		(AclAccessType.READ_WHITELIST, myGuid, Constants.TIME_REFRESH_INTERVAL, null) );
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.fieldReplaceOrCreateList
    		(guid.getGuid(), Constants.CURRENT_TIME, new JSONArray().put(now), myGuid) );
    
    DefaultGNSClient.getGnsClient().execute( GNSCommand.aclAdd
    		(AclAccessType.READ_WHITELIST, myGuid, Constants.CURRENT_TIME, null) );
    logger.setLevel(Level.FINE);
  }

  /**
   * Call this method to terminate the thread
   */
  public void killIt()
  {
    killed = true;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    long last = System.currentTimeMillis();
    while (!killed)
    {
      long sleepFor = last + publishFrequency - System.currentTimeMillis();
      if (sleepFor > 0)
        try
        {
          Thread.sleep(sleepFor);
        }
        catch (InterruptedException ignore)
        {
        }

      last = System.currentTimeMillis();
      try
      {
    	  DefaultGNSClient.getGnsClient().execute(GNSCommand.fieldReplaceList
    			  ( guid.getGuid(), Constants.CURRENT_TIME,
    	            new JSONArray().put(last), guid) );
    	  
        logger.fine("Updated current time to " + last);
      }
      catch (Exception e)
      {
        logger.log(Level.WARNING, "Failed to update CURRENT_TIME with " + last, e);
      }
    }
  }

}
