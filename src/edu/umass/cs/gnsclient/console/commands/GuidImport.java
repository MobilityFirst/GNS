
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.security.KeyPair;
import java.util.StringTokenizer;


public class GuidImport extends ConsoleCommand
{


  public GuidImport(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Load an alias/GUID and keypair information from a file on disk";
  }

  @Override
  public String getCommandName()
  {
    return "guid_import";
  }

  @Override
  public String getCommandParameters()
  {
    return "path_and_filename";
  }


  @Override
  public void execute(String commandText) throws Exception
  {
    parse(commandText);
  }

  @Override
  public void parse(String commandText) throws Exception
  {
    try
    {
      StringTokenizer st = new StringTokenizer(commandText.trim());
      if (st.countTokens() != 1)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String filename = st.nextToken();

      File f = new File(filename);
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
      GuidEntry guidEntry = new GuidEntry(ois);
      ois.close();
      KeyPairUtils.saveKeyPair(module.getGnsInstance(), guidEntry.getEntityName(), guidEntry.getGuid(),
          new KeyPair(guidEntry.getPublicKey(), guidEntry.getPrivateKey()));
      console.printString("Keys for " + guidEntry.getEntityName() + " read from " + filename
          + " and saved in local preferences.");
      console.printNewline();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      console.printString("Failed to load keys ( " + e + ")\n");
    }
  }

}
