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

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.security.KeyPair;
import java.util.StringTokenizer;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import java.io.IOException;

/**
 * Command that reads GUID/alias/Keypair information from a file
 */
public class GuidImport extends ConsoleCommand {

  /**
   * Creates a new <code>GuidImport</code> object
   *
   * @param module
   */
  public GuidImport(ConsoleModule module) {
    super(module);
  }

  @Override
  public String getCommandDescription() {
    return "Load an alias/GUID and keypair information from a file on disk";
  }

  @Override
  public String getCommandName() {
    return "guid_import";
  }

  @Override
  public String getCommandParameters() {
    return "path_and_filename";
  }

  /**
   * Override execute to not check for existing connectivity
   *
   * @throws java.lang.Exception
   */
  @Override
  public void execute(String commandText) throws Exception {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception {
    try {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 1) {
        wrongArguments();
        return;
      }
      String filename = st.nextToken();

      File f = new File(filename);
      GuidEntry guidEntry;
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
        guidEntry = new GuidEntry(ois);
        ois.close();
      }
      KeyPairUtils.saveKeyPair(module.getGnsInstance(), guidEntry.getEntityName(), guidEntry.getGuid(),
              new KeyPair(guidEntry.getPublicKey(), guidEntry.getPrivateKey()));
      console.printString("Keys for " + guidEntry.getEntityName() + " read from " + filename
              + " and saved in local preferences.");
      console.printNewline();
    } catch (IOException | EncryptionException e) {
      console.printString("Failed to load keys ( " + e + ")\n");
    }
  }

}
