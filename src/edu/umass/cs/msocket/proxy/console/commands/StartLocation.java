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

package edu.umass.cs.msocket.proxy.console.commands;


import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.msocket.gns.DefaultGNSClient;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;
import edu.umass.cs.msocket.proxy.location.LocationService;

/**
 * Command that starts a new location service
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StartLocation extends ConsoleCommand
{

  /**
   * Creates a new <code>StartLocationService</code> object
   * 
   * @param module
   */
  public StartLocation(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Start a location service";
  }

  @Override
  public String getCommandName()
  {
    return "location_start";
  }

  @Override
  public String getCommandParameters()
  {
    return "location_service_name";
  }

  /**
   * Override execute to not check for existing connectivity
   */
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    if (module.getRunningLocationService() != null)
    {
      console.printString("A location service is already running.\n");
      return;
    }
    if (module.getProxyGroupGuid() == null)
    {
      console.printString("You have to connect to a proxy group first.\n");
      return;
    }
    try
    {
      String serviceName = commandText.trim();

      if (!module.isSilent())
        console.printString("Looking for location service  " + serviceName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsClient().getGNSProvider(), serviceName);

      if (myGuid == null)
      {
        if (!module.isSilent())
          console.printString("No keys found for location service " + serviceName + ". Generating new GUID and keys\n");
        
        GNSCommand commandRes = DefaultGNSClient.getGnsClient().execute(GNSCommand.createGUID
        		(DefaultGNSClient.getGnsClient().getGNSProvider(), module.getAccountGuid(), serviceName));
        
        
        myGuid = (GuidEntry) commandRes.getResult();
      }
      
      if (myGuid == null)
      {
        console.printString("No keys found for location service " + serviceName + ". Cannot connect without the key\n");
        return;
      }
      if (!module.isSilent())
      {
        console.printString("Location service has guid " + myGuid.getGuid() + "\n");
        console.printString("Starting location service\n");
      }
      LocationService locationer = new LocationService(module.getProxyGroupGuid().getEntityName(), serviceName);
      locationer.registerLocationServiceInGns();
      locationer.start();
      module.setRunningLocationService(locationer);
    }
    catch (Exception e)
    {
      console.printString("Failed to start location service ( " + e + ")\n");
      e.printStackTrace();
    }
  }
}
