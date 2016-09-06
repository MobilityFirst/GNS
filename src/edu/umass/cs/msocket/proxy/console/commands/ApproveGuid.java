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
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.msocket.common.Constants;
import edu.umass.cs.msocket.proxy.console.ConsoleModule;

/**
 * This class defines a ApproveGuid
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ApproveGuid extends ConsoleCommand
{

  /**
   * Creates a new <code>ApproveGuid</code> object
   * 
   * @param module
   */
  public ApproveGuid(ConsoleModule module)
  {
    super(module);
  }

  /**
   * @see edu.umass.cs.msocket.proxy.console.commands.ConsoleCommand#getCommandName()
   */
  @Override
  public String getCommandName()
  {
    return "approve_guid";
  }

  /**
   * @see edu.umass.cs.msocket.proxy.console.commands.ConsoleCommand#getCommandParameters()
   */
  @Override
  public String getCommandParameters()
  {
    return "guid";
  }

  /**
   * @see edu.umass.cs.msocket.proxy.console.commands.ConsoleCommand#getCommandDescription()
   */
  @Override
  public String getCommandDescription()
  {
    return "Approve the GUID of the service that is requesting to join the group (use list_guids UNVERIFIED to find list of pending requests).";
  }

  /**
   * @see edu.umass.cs.msocket.proxy.console.commands.ConsoleCommand#parse(java.lang.String)
   */
  @Override
  public void parse(String commandText) throws Exception
  {
    String guid = null;
    try
    {
      StringTokenizer st = new StringTokenizer(commandText);
      if (st.countTokens() != 1)
      {
        console.printString("Bad number of arguments (expected 1 instead of " + st.countTokens() + ")\n");
        return;
      }

      final GuidEntry groupGuid = module.getProxyGroupGuid();
      if (groupGuid == null)
      {
        console.printString("Not connected to a proxy group. Use proxy_group_connect or help for instructions.\n");
        return;
      }

      guid = st.nextToken();

      GNSClientCommands gnsClient = module.getGnsClient();
      gnsClient.groupAddGuid(groupGuid.getGuid(), guid, groupGuid);

      /*
       * Check what kind of service this GUID represents and put it in the
       * appropriate list
       */
      String service = gnsClient.fieldReadArray(guid, Constants.SERVICE_TYPE_FIELD, groupGuid).getString(0);
      if (Constants.PROXY_SERVICE.equals(service))
      {
        console.printString("Granting access to proxy " + guid
            + " and moving it to the inactive proxy list. Make sure a watchdog is running to detect its activity.\n");
        gnsClient.fieldAppend(module.getProxyGroupGuid().getGuid(), Constants.INACTIVE_PROXY_FIELD,
            new JSONArray().put(guid), groupGuid);
      }
      else if (Constants.LOCATION_SERVICE.equals(service))
      {
        console.printString("Granting access to location service " + guid
            + " and moving it to the inactive service list. Make sure a watchdog is running to detect its activity.\n");
        gnsClient.fieldAppend(groupGuid.getGuid(), Constants.INACTIVE_LOCATION_FIELD, new JSONArray().put(guid),
            groupGuid);
        gnsClient.aclAdd(AclAccessType.READ_WHITELIST, groupGuid, Constants.ACTIVE_PROXY_FIELD, guid);
      }
      else if (Constants.WATCHDOG_SERVICE.equals(service))
      {
        console
            .printString("Granting access to watchdog service "
                + guid
                + " and moving it to the inactive wachdog list. Make sure another watchdog is running to detect its activity.\n");
        gnsClient.fieldAppend(groupGuid.getGuid(), Constants.INACTIVE_WATCHDOG_FIELD, new JSONArray().put(guid),
            groupGuid);
        // Open lists in read/write for the watchdog so that it can manipulate
        // lists
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.ACTIVE_PROXY_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.SUSPICIOUS_PROXY_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.INACTIVE_PROXY_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.ACTIVE_LOCATION_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.SUSPICIOUS_LOCATION_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.INACTIVE_LOCATION_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.ACTIVE_WATCHDOG_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.SUSPICIOUS_WATCHDOG_FIELD);
        setReadWriteAccess(guid, groupGuid, gnsClient, Constants.INACTIVE_WATCHDOG_FIELD);
      }

    }
    catch (Exception e)
    {
      console.printString("Failed to grant permission to join the proxy group to GUID" + guid + " ( " + e + ")\n");
      e.printStackTrace();
    }
  }

  private void setReadWriteAccess(String guid, final GuidEntry myGuid, GNSClientCommands gnsClient, String field)
      throws Exception
  {
    gnsClient.aclAdd(AclAccessType.READ_WHITELIST, myGuid, field, guid);
    gnsClient.aclAdd(AclAccessType.WRITE_WHITELIST, myGuid, field, guid);
  }
}
