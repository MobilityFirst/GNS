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

package edu.umass.cs.msocket.proxy.location;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.msocket.common.proxy.policies.ProxySelectionPolicy;

/**
 * This class defines a LocateServiceThread
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class LocateServiceThread extends Thread
{
  private Socket              acceptedSocket;
  private LocationService     locationService;
  private static final Logger logger = Logger.getLogger("LocationService");

  /**
   * Creates a new <code>LocateServiceThread</code> object
   * 
   * @param acceptedSocket
   * @param locationService
   */
  public LocateServiceThread(Socket acceptedSocket, LocationService locationService)
  {
    this.acceptedSocket = acceptedSocket;
    this.locationService = locationService;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run()
  {
    try
    {
      // Now read the proxy policy
      ObjectInputStream ois = new ObjectInputStream(acceptedSocket.getInputStream());
      ObjectOutputStream oos = new ObjectOutputStream(acceptedSocket.getOutputStream());

      ProxySelectionPolicy psp = (ProxySelectionPolicy) ois.readObject();

      List<String> ips = psp.getProxyIPs(locationService.getStatusThread().getProxies(), acceptedSocket);

      for (String ipPort : ips)
      {
        oos.writeUTF(ipPort);
        oos.flush();
      }
    }
    catch (Exception e)
    {
      logger.log(Level.WARNING, "Failed to process requested proxy policy", e);
    }
    finally
    {
      try
      {
        acceptedSocket.close();
      }
      catch (IOException ignore)
      {
      }
    }

  }

}
