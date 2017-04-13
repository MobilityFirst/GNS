/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.console.commands;

import java.security.PublicKey;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.InvalidGuidException;

import java.security.cert.X509Certificate;
import java.util.StringTokenizer;

/**
 * Create a new account
 */
public class AccountCreateWithCertificate extends ConsoleCommand
{

  /**
   * Creates a new <code>AccountCreate</code> object
   * 
   * @param module
   */
  public AccountCreateWithCertificate(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Create a new account which includes a GUID, a human readable name , certificate associates it with the alias and register it in the GNS.";
  }

  @Override
  public String getCommandName()
  {
    return "account_create_with_certificate";
  }

  @Override
  public String getCommandParameters()
  {
    return "certificatepath privatekeypath password";
  }

  @Override
  public void parse(String commandText) throws Exception
  {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      printString("I am here \n");
      if ((st.countTokens() != 3))
      {
        wrongArguments();
        return;
      }
      String certificatePath = st.nextToken();
      String privateKeyPath = st.nextToken();
      String password = st.nextToken();

      X509Certificate cert = SharedGuidUtils.loadCertificateFromFile(certificatePath);
      String aliasName = SharedGuidUtils.getNameFromCertificate(cert);


    try
    {
      GNSClient gnsClient = module.getGnsClient();

      try
      {
        gnsClient.execute(GNSCommand.lookupGUID(aliasName)).getResultString();
        if (!module.isSilent())
        {
          printString("Alias " + aliasName + " already exists.\n");
        }
        return;
      }
      catch (Exception expected)
      {
        // The alias does not exists, that's good, let's create it
      }
      GuidEntry myGuid =  GuidUtils.accountGuidCreateWithCertificate(gnsClient, password,certificatePath, privateKeyPath );

      if (!module.isSilent())
      {
        printString("Created an account with GUID " + myGuid.getGuid() + ".\n"
//                + "An email might have been sent to "
//            + myGuid.getEntityName() + " with instructions on how to verify the new account.\n"
        );
      }
      if (module.getCurrentGuid() == null)
      {
        module.setCurrentGuidAndCheckForVerified(myGuid);
        if (KeyPairUtils.getDefaultGuidEntry(module.getGnsInstance()) == null)
        {
          KeyPairUtils.setDefaultGuidEntry(module.getGnsInstance(), aliasName);
          module.printString(aliasName + " saved as default GUID \n");
        }
        module.setPromptString(ConsoleModule.CONSOLE_PROMPT + aliasName + ">");
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      printString("Failed to create new guid ( " + e + ")\n");
    }
  }
}
