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

import java.util.StringTokenizer;

import org.json.JSONArray;







import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;
import edu.umass.cs.msocket.proxy.watchdog.Watchdog;

/**
 * Command that starts a new watchdog
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class StartWatchdog extends ConsoleCommand
{

  /**
   * Creates a new <code>StartWatchdog</code> object
   * 
   * @param module
   */
  public StartWatchdog(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Starts a watchdog service that will scan the list of proxies, location services or watchdogs (or all of them) at the given interval and use the new/suspicious/failure timeouts to update their statuses accordingly";
  }

  @Override
  public String getCommandName()
  {
    return "watchdog_start";
  }

  @Override
  public String getCommandParameters()
  {
    return "[proxy|location|watchdog|all] watchdog_name new_lookup_interval_in_ms suspicious_timeout_in_ms failure_timeout_in_ms";
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
    if (module.getProxyGroupGuid() == null)
    {
      console.printString("You have to connect to a proxy group first.\n");
      return;
    }

    // Check if we already have a watchdog with that name running
    try
    {
      StringTokenizer st = new StringTokenizer(commandText);
      if (st.countTokens() != 5)
      {
        console.printString("Bad number of arguments (expected 5 instead of " + st.countTokens() + ")\n");
        return;
      }

      String targetList = st.nextToken();
      String watchdogName = st.nextToken();
      long lookupIntervalInMs = Long.parseLong(st.nextToken());
      long suspiciousTimeoutInMs = Long.parseLong(st.nextToken());
      long failureTimeoutInMs = Long.parseLong(st.nextToken());

      if (!"proxy".equalsIgnoreCase(targetList) && !"location".equalsIgnoreCase(targetList)
          && !"watchdog".equalsIgnoreCase(targetList) && !"all".equalsIgnoreCase(targetList))
      {
        console.printString("Invalid list '" + targetList + "'. You must pick one of proxy, location, watchdog or all");
        return;
      }

      if (suspiciousTimeoutInMs <= 0 || failureTimeoutInMs <= 0 || failureTimeoutInMs < suspiciousTimeoutInMs)
      {
        console
            .printString("Timeouts must have positive values and failure timeout must be greater than suspicious timeout.");
        return;
      }
      if (lookupIntervalInMs <= 0)
      {
        console.printString("Lookup interval must be a positive number.");
        return;
      }

      if (!module.isSilent())
        console.printString("Looking for watchdog  " + watchdogName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsClient().getGNSProvider(), watchdogName);
      final GNSClientCommands gnsClient = module.getGnsClient();

      if (myGuid == null)
      {
        if (!module.isSilent())
          console.printString("No keys found for watchdog " + watchdogName + ". Generating new GUID and keys\n");
        myGuid = gnsClient.guidCreate(module.getAccountGuid(), watchdogName);
      }

      if (myGuid == null)
      {
        console.printString("No keys found for watchdog " + watchdogName + ". Cannot connect without the key\n");
        return;
      }
      if (!module.isSilent())
        console.printString("Watchdog has guid " + myGuid.getGuid());

      // Make sure we advertise ourselves as a watchdog (readable for everyone)
      gnsClient.fieldReplaceOrCreateList(myGuid.getGuid(), Constants.SERVICE_TYPE_FIELD,
          new JSONArray().put(Constants.WATCHDOG_SERVICE), myGuid);
      gnsClient.aclAdd(AclAccessType.READ_WHITELIST, myGuid, Constants.SERVICE_TYPE_FIELD, null);

      // Check if we are a member of the group
      final String groupGuid = module.getProxyGroupGuid().getGuid();
      JSONArray members = gnsClient.groupGetMembers(groupGuid, myGuid);
      boolean isVerified = false;
      for (int i = 0; i < members.length(); i++)
      {
        if (myGuid.getGuid().equals(members.get(i)))
        {
          isVerified = true;
          break;
        }
      }

      if (!isVerified)
      {
        console
            .printString("This watchdog has not been verified yet, it will stay in the unverified list until it gets added to the proxy group");
      }

      // And now start 3 watchdogs: 1 for proxies, 1 for location services and 1
      // for other watchdogs
      if ("proxy".equalsIgnoreCase(targetList) || "all".equalsIgnoreCase(targetList))
      {
        Watchdog proxyWatchdog = new Watchdog(gnsClient, myGuid, groupGuid, lookupIntervalInMs,
            Constants.ACTIVE_PROXY_FIELD, Constants.SUSPICIOUS_PROXY_FIELD, Constants.INACTIVE_PROXY_FIELD,
            suspiciousTimeoutInMs, failureTimeoutInMs);
        module.setRunningProxyWatchdog(proxyWatchdog);
        proxyWatchdog.start();
      }
      if ("location".equalsIgnoreCase(targetList) || "all".equalsIgnoreCase(targetList))
      {
        Watchdog locationWatchdog = new Watchdog(gnsClient, myGuid, groupGuid, lookupIntervalInMs,
            Constants.ACTIVE_LOCATION_FIELD, Constants.SUSPICIOUS_LOCATION_FIELD,
            Constants.INACTIVE_LOCATION_FIELD, suspiciousTimeoutInMs, failureTimeoutInMs);
        module.setRunningLocationWatchdog(locationWatchdog);
        locationWatchdog.start();
      }
      if ("watchdog".equalsIgnoreCase(targetList) || "all".equalsIgnoreCase(targetList))
      {
        Watchdog watchdogWatchdog = new Watchdog(gnsClient, myGuid, groupGuid, lookupIntervalInMs,
            Constants.ACTIVE_WATCHDOG_FIELD, Constants.SUSPICIOUS_WATCHDOG_FIELD,
            Constants.INACTIVE_WATCHDOG_FIELD, suspiciousTimeoutInMs, failureTimeoutInMs);
        module.setRunningWatchdogWatchdog(watchdogWatchdog);
        watchdogWatchdog.start();
      }
    }
    catch (Exception e)
    {
      console.printString("Failed to coonect to start watchdog service ( " + e + ")\n");
    }
  }
}
