
package edu.umass.cs.gnsclient.console.commands;

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.ConsoleModule;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.StringTokenizer;


public class GuidExport extends ConsoleCommand
{


  public GuidExport(ConsoleModule module)
  {
    super(module);
  }

  @Override
  public String getCommandDescription()
  {
    return "Saves alias/GUID and keypair information into a file on disk (careful, the file is not encrypted)";
  }

  @Override
  public String getCommandName()
  {
    return "guid_export";
  }

  @Override
  public String getCommandParameters()
  {
    return "alias path_and_filename";
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
      if (st.countTokens() != 2)
      {
        console.printString("Wrong number of arguments for this command.\n");
        return;
      }
      String aliasName = st.nextToken();
      String filename = st.nextToken();

      if (!module.isSilent())
        console.printString("Looking up alias " + aliasName + " GUID and certificates...\n");
      GuidEntry myGuid = KeyPairUtils.getGuidEntry(module.getGnsInstance(), aliasName);

      if (myGuid == null)
      {
        console.printString("You do not have the private key for alias " + aliasName);
        console.printNewline();
        return;
      }

      File f = new File(filename);
      f.createNewFile();
      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
      myGuid.writeObject(oos);
      oos.flush();
      oos.close();
      console.printString("Keys for " + aliasName + " stored in " + filename);
      console.printNewline();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      console.printString("Failed to save keys ( " + e + ")\n");
    }
  }
}
