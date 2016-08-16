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


import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.msocket.proxy.ProxyPublisher;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;
import edu.umass.cs.msocket.proxy.forwarder.ProxyForwarder;

/**
 * Command that starts a new proxy
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StartProxy extends ConsoleCommand
{

  /**
   * Creates a new <code>StartProxy</code> object
   * 
   * @param module
   */
  public StartProxy(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Start a proxy";
  }

  @Override
  public String getCommandName()
  {
    return "proxy_start";
  }

  @Override
  public String getCommandParameters()
  {
    return "proxy_name";
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
    if (module.getRunningProxy() != null)
    {
      console.printString("A proxy is already running.\n");
      return;
    }
    if (module.getProxyGroupGuid() == null)
    {
      console.printString("You have to connect to a proxy group first.\n");
      return;
    }
    try
    {
      String proxyName = commandText.trim();
      if (proxyName.isEmpty())
      {
        console.printString("Cannot use an empty proxy name.\n");
        return;
      }

      if (!module.isSilent())
        console.printString("Looking for proxy  " + proxyName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsClient().getGNSProvider(), proxyName);
      final GNSClientCommands gnsClient = module.getGnsClient();

      if (myGuid == null)
      {
        if (!module.isSilent())
          console.printString("No keys found for proxy " + proxyName
              + ". Generating new GUID and keys using account GUID " + module.getAccountGuid() + "\n");
        myGuid = gnsClient.guidCreate(module.getAccountGuid(), proxyName);
      }

      if (myGuid == null)
      {
        console.printString("No keys found for proxy " + proxyName + ". Cannot connect without the key\n");
        return;
      }
      if (!module.isSilent())
      {
        console.printString("Proxy has guid " + myGuid.getGuid() + "\n");
        console.printString("Starting proxy forwarder\n");
      }
      ProxyForwarder forwarder = new ProxyForwarder(proxyName, 0);
      ProxyPublisher proxy = new ProxyPublisher(
          proxyName, module.getProxyGroupGuid().getEntityName(), forwarder.getProxyListeningAddress());
      proxy.registerProxyInGns();
      proxy.start();
      module.setRunningProxy(proxy);
    }
    catch (Exception e)
    {
      console.printString("Failed to start proxy ( " + e + ")\n");
      e.printStackTrace();
    }
  }
}
