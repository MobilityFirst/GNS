
package edu.umass.cs.gnsclient.console;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.commands.Connect;
import edu.umass.cs.gnsclient.console.commands.ConsoleCommand;
import edu.umass.cs.gnsclient.console.commands.GuidUse;
import edu.umass.cs.gnsclient.console.commands.Help;
import edu.umass.cs.gnsclient.console.commands.History;
import edu.umass.cs.gnsclient.console.commands.Quit;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.SimpleCompletor;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.prefs.Preferences;


public class ConsoleModule {

  private final ConsoleReader console;
  private final TreeSet<ConsoleCommand> commands;
  private boolean quit = false;
  private boolean useGnsDefaults = true;


  @SuppressWarnings("javadoc")
  protected Completor consoleCompletor;
  private String promptString = CONSOLE_PROMPT + "not connected>";
  private GNSClientCommands gnsClient;
  private GuidEntry currentGuid;
  // might be a better way to do this, but for now
  private boolean accountVerified;
  private boolean silent;


  public static final String CONSOLE_PROMPT = "GNS CLI - ";

  public static String DEFAULT_COMMAND_PROPERTIES_FILE = "edu/umass/cs/gnsclient/console/console.properties";


  public ConsoleModule(ConsoleReader console) {
    this.console = console;
    this.commands = new TreeSet<>();
    commands.add(new Help(this));
    commands.add(new History(this));
    commands.add(new Quit(this));
    this.loadCommands();
    this.loadCompletor();
    console.addCompletor(getCompletor());
    console.setHistory(loadHistory());

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        GNSClientConfig.getLogger().info("Saving history.");
        long startTime = System.currentTimeMillis();
        storeHistory();
        GNSClientConfig.getLogger().log(Level.INFO, "Save history took {0}ms",
                (System.currentTimeMillis() - startTime));
      }
    });
  }

  private jline.History loadHistory() {
    jline.History jHistory = new jline.History();
    try {
      Preferences prefs = Preferences.userRoot().node(this.getClass().getName());
      String[] historyKeys = prefs.keys();
      Arrays.sort(historyKeys, 0, historyKeys.length);
      GNSClientConfig.getLogger().log(Level.INFO, "Loading history. Size is {0}", historyKeys.length);
      for (int i = 0; i < historyKeys.length; i++) {
        String key = historyKeys[i];
        String value = prefs.get(key, ""); //$NON-NLS-1$
        jHistory.addToHistory(value);
      }
    } catch (Exception e) {
      // unable to load prefs: do nothing
    }
    return jHistory;
  }


  @SuppressWarnings("unchecked")
  public List<String> getHistory() {
    return console.getHistory().getHistoryList();
  }

  private final static int NUMBER_OF_HISTORY_ITEMS_TO_STORE = 20;


  public void storeHistory() {
    @SuppressWarnings("unchecked")
    List<String> history = console.getHistory().getHistoryList();
    try {
      Preferences prefs = Preferences.userRoot().node(this.getClass().getName());
      prefs.clear();
      int historySize = history.size();
      int start = Math.max(0, historySize - NUMBER_OF_HISTORY_ITEMS_TO_STORE);
      // save up to the last 100th history items only
      // with the stored index starting at 0
      for (int i = start; i < historySize; i++) {
        prefs.put(String.valueOf(i - start), history.get(i + start));
      }
      prefs.flush();
    } catch (Exception e) {
      // unable to store prefs: do nothing
    }
  }


  protected final void loadCommands() {
    commands.clear();
    String commandClassesAsString = loadCommandsFromProperties("main");
    String[] commandClasses = parseCommands(commandClassesAsString);
    addCommands(commandClasses, commands);
  }


  protected String[] parseCommands(String commandClassesAsString) {
    if (commandClassesAsString == null) {
      return new String[0];
    }
    String[] cmds = commandClassesAsString.split("\\s*,\\s*"); //$NON-NLS-1$
    return cmds;
  }


  protected void addCommands(String[] commandClasses, Set<ConsoleCommand> commands) {
    for (int i = 0; i < commandClasses.length; i++) {
      String commandClass = commandClasses[i].trim();
      Class<?> clazz;
      try {
        clazz = Class.forName(commandClass);
        Constructor<?> constructor;
        try {
          constructor = clazz.getConstructor(new Class<?>[]{this.getClass()});
        } catch (NoSuchMethodException e) {
          constructor = clazz.getConstructor(new Class<?>[]{ConsoleModule.class});
        }
        ConsoleCommand command = (ConsoleCommand) constructor.newInstance(new Object[]{this});
        commands.add(command);
      } catch (ClassNotFoundException | SecurityException | NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        // fail silently: the command won't be added to the commands list
      }
    }
  }


  protected String loadCommandsFromProperties(String moduleID) {

    //System.out.println(this.getClass().getClassLoader().getResource(".").getPath());
    Properties props = new Properties();
    try {
      String propertiesFile = System.getProperty("console.commands", DEFAULT_COMMAND_PROPERTIES_FILE);
      GNSClientConfig.getLogger().log(Level.INFO, "Loading commands from {0}", propertiesFile);
      InputStream is = ClassLoader.getSystemResourceAsStream(propertiesFile);
//      GNSClientConfig.getLogger().warning("THREAD: "
//              + Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFile));
      if (is == null) {
        GNSClientConfig.getLogger().log(Level.WARNING, "Unable to load from {0}", propertiesFile);
        return null;
      }
      props.load(is);
    } catch (IOException e) {
      // fail silently: no commands will be loaded
    } catch (RuntimeException e) {
      GNSClientConfig.getLogger().log(Level.INFO, "Unable to load commands: {0}", e.getMessage());
      e.printStackTrace();
    }
    String commandClassesAsString = props.getProperty(moduleID, "");
    return commandClassesAsString;
  }


  protected void loadCompletor() {
    List<Completor> completors = new LinkedList<>();
    int size = commands.size();
    if (size > 0) {
      TreeSet<String> set = new TreeSet<>();
      Iterator<ConsoleCommand> it = commands.iterator();
      while (it.hasNext()) {
        set.add(it.next().getCommandName());
      }
      completors.add(new SimpleCompletor(set.toArray(new String[size])));
    }
    completors.add(new FileNameCompletor());

    Completor[] completorsArray = completors.toArray(new Completor[completors.size()]);
    consoleCompletor = new ArgumentCompletor(completorsArray, new CommandDelimiter());
  }


  protected synchronized void reloadCompletor() {
    console.removeCompletor(consoleCompletor);
    loadCompletor();
    console.addCompletor(consoleCompletor);
  }


  public void help() {
    printString("Commands available for the main menu are:\n");
    ConsoleCommand command;
    Iterator<ConsoleCommand> it = commands.iterator();
    while (it.hasNext()) {
      command = it.next();
      printString(command.getCommandName() + " " + command.getCommandParameters() + "\n");
      printString("   " + command.getCommandDescription() + "\n");
    }
  }


  public void quit() {
    quit = true;
    console.removeCompletor(getCompletor());
  }


  public TreeSet<ConsoleCommand> getCommands() {
    return commands;
  }


  public String getPromptString() {
    if (silent) {
      return "";
    }
    return promptString;
  }


  public void setPromptString(String promptString) {
    this.promptString = promptString;
  }


  public void handlePrompt() {
    quit = false;
    // Try to connect to the GNS
    try {
      new Connect(this).parse("");
    } catch (Exception e) {
      printString("Couldn't connect to GNS...\n");
    }
    if (useGnsDefaults) {
      useGnsDefaults();
    }
    while (!quit) {

      Hashtable<String, ConsoleCommand> hashCommands = getHashCommands();
      try {
        String commandLine;
        if (silent) {
          console.flushConsole();
          commandLine = readLineBypassJLine();
        } else {
          commandLine = console.readLine(getPromptString());
        }

        if (commandLine == null) {
          quit();
          break;
        }
        if (commandLine.equals("")) {
          continue;
        }

        handleCommandLine(commandLine, hashCommands);
      } catch (Exception e) {
        printString("Error during console commnand execution: " + e.getMessage() + "\n");

      }
    }
    GNSClientConfig.getLogger().fine("Quitting");
  }


  public void useGnsDefaults() {
    GuidEntry guid = KeyPairUtils.getDefaultGuidEntry(getGnsInstance());
    if (guid == null) {
      return;
    }
    try {
      printString("Looking for default GUID: " + guid + "\n");
      new GuidUse(this).parse(guid.getEntityName());
    } catch (Exception e) {
      printString("Couldn't connect default GUID " + guid);
    }
  }

  private boolean lastWasCR = false;
  private List<Byte> currentLine = new ArrayList<>();


  private String readLineBypassJLine() throws IOException {
    // If JLine implements any kind of internal read buffering, we
    // are screwed.
    InputStream jlineInternal = console.getInput();


    // Reader jlineInternal = new InputStreamReader(consoleReader.getInput());
    currentLine.clear();

    int ch = jlineInternal.read();

    if (ch == -1  || ch == 4 ) {
      return null;
    }


    // discard any LF following a CR
    if (lastWasCR && ch == '\n') {
      ch = jlineInternal.read();
    }

    // EOF also counts as an end of line. Not sure this is what JLine does but
    // it looks good.
    while (ch != -1 && ch != '\n' && ch != '\r') {
      currentLine.add((byte) ch);
      ch = jlineInternal.read();
    }

    // SEQUOIA-911 FIXME: we may have found a '\n' or '\r' INSIDE a multibyte
    // character. Definitely not a real newline.
    lastWasCR = (ch == '\r');

    // "cast" byte List into a primitive byte array
    byte[] encoded = new byte[currentLine.size()];
    Iterator<Byte> it = currentLine.iterator();
    for (int i = 0; it.hasNext(); i++) {
      encoded[i] = it.next();
    }


    String line = new String(encoded);

    return line;
  }


  public final Hashtable<String, ConsoleCommand> getHashCommands() {
    Hashtable<String, ConsoleCommand> hashCommands = new Hashtable<String, ConsoleCommand>();
    for (ConsoleCommand consoleCommand : commands) {
      hashCommands.put(consoleCommand.getCommandName(), consoleCommand);
    }
    return hashCommands;
  }


  public final void handleCommandLine(String commandLine, Hashtable<String, ConsoleCommand> hashCommands)
          throws Exception {
    StringTokenizer st = new StringTokenizer(commandLine);
    if (st.hasMoreTokens()) {
      ConsoleCommand command = findConsoleCommand(commandLine, hashCommands);
      GNSClientConfig.getLogger().log(Level.FINE, "Command:{0}", command);
      if (command != null) {
        command.execute(commandLine.substring(command.getCommandName().length()));
        return;
      }
    }
    throw new Exception("Command " + commandLine + " is not supported here");
  }


  public ConsoleCommand findConsoleCommand(String commandLine, Hashtable<String, ConsoleCommand> hashCommands) {
    ConsoleCommand foundCommand = null;
    for (Iterator<?> iter = hashCommands.entrySet().iterator(); iter.hasNext();) {
      @SuppressWarnings("rawtypes")
      Map.Entry commandEntry = (Map.Entry) iter.next();
      String commandName = (String) commandEntry.getKey();
      if (commandLine.startsWith(commandName)) {
        ConsoleCommand command = (ConsoleCommand) commandEntry.getValue();
        if (foundCommand == null) {
          foundCommand = command;
        }
        if (command.getCommandName().length() > foundCommand.getCommandName().length()) {
          foundCommand = command;
        }
      }
    }
    return foundCommand;
  }


  public ConsoleReader getConsole() {
    return console;
  }


  public Completor getCompletor() {
    return consoleCompletor;
  }


  public boolean isSilent() {
    return silent;
  }


  public void setSilent(boolean silent) {
    this.silent = silent;
  }


  public GNSClientCommands getGnsClient() {
    return gnsClient;
  }


  public void setGnsClient(GNSClientCommands gnsClient) {
    this.gnsClient = gnsClient;
  }


  public GuidEntry getCurrentGuid() {
    return currentGuid;
  }


  public boolean isAccountVerified() {
    return accountVerified;
  }


  public void setAccountVerified(boolean accountVerified) {
    this.accountVerified = accountVerified;
  }


  public void setCurrentGuid(GuidEntry guid) {
    this.currentGuid = guid;
  }


  public boolean isCurrentGuidSetAndVerified() {
    if (currentGuid == null) {
      printString("Select the GUID to use first with guid_use.\n");
      return false;
    } else if (!accountVerified) {
      if (checkGnsIsAccountVerified(currentGuid)) {
        accountVerified = true;
        return true;
      }
      printString(currentGuid.getEntityName() + " is not verified.\n");
      return false;
    }
    return true;
  }


  public void setCurrentGuidAndCheckForVerified(GuidEntry guid) {
    this.currentGuid = guid;
    if (gnsClient != null) {
      accountVerified = checkGnsIsAccountVerified(currentGuid);
    }
    if (!accountVerified && currentGuid != null) {
      printString(currentGuid.getEntityName() + " is not verified.\n");
    }
  }


  public void setDefaultGuidAndCheckForVerified(GuidEntry guid) {
    KeyPairUtils.setDefaultGuidEntry(getGnsInstance(), guid.getEntityName());
    if (currentGuid == null) {
      currentGuid = guid;
    }
    if (gnsClient != null) {
      accountVerified = checkGnsIsAccountVerified(currentGuid);
    }
    if (!accountVerified) {
      printString(currentGuid.getEntityName() + " is not verified.\n");
    }
  }


  public String getGnsInstance() {
    if (gnsClient == null) {
      return null;
    } else {
      return gnsClient.getGNSProvider();
    }
  }


  public boolean checkGnsIsAccountVerified(GuidEntry guid) {
    try {
      JSONObject json = gnsClient.lookupAccountRecord(guid.getGuid());
      if (json != null) {
        return json.getBoolean(GNSProtocol.ACCOUNT_RECORD_VERIFIED.toString());
      } else { // This is not an account GNSProtocol.GUID.toString() but make sure the GNSProtocol.GUID.toString() is valid
        return gnsClient.publicKeyLookupFromGuid(guid.getGuid()) != null;
      }
    } catch (IOException | ClientException | JSONException e) {
      // This might not be an account GNSProtocol.GUID.toString() let's check if the GNSProtocol.GUID.toString() is valid
      try {
        return gnsClient.publicKeyLookupFromGuid(guid.getGuid()) != null;
      } catch (ClientException | IOException e1) {
        return false;
      }
    }
  }


  public void printString(String string) {
    try {
      console.printString(string);
    } catch (IOException e) {
      GNSClientConfig.getLogger().warning("Problem printing string to console: " + e);
    }
  }


  private class CommandDelimiter extends ArgumentCompletor.AbstractArgumentDelimiter {


    @Override
    public boolean isDelimiterChar(String buffer, int pos) {
      String tentativeCmd = buffer.substring(0, pos);
      return isACompleteCommand(tentativeCmd);
    }


    private boolean isACompleteCommand(String input) {
      boolean foundCompleteCommand = false;
      for (Iterator<ConsoleCommand> iter = commands.iterator(); iter.hasNext();) {
        ConsoleCommand command = iter.next();
        if (input.equals(command.getCommandName())) {
          foundCompleteCommand = !otherCommandsStartWith(command.getCommandName());
        }
      }
      return foundCompleteCommand;
    }

    private boolean otherCommandsStartWith(String commandName) {
      for (Iterator<ConsoleCommand> iter = commands.iterator(); iter.hasNext();) {
        ConsoleCommand command = iter.next();
        if (command.getCommandName().startsWith(commandName) && !command.getCommandName().equals(commandName)) {
          return true;
        }
      }
      return false;
    }
  }


  public boolean isUseGnsDefaults() {
    return useGnsDefaults;
  }


  public void setUseGnsDefaults(boolean useGnsDefaults) {
    this.useGnsDefaults = useGnsDefaults;
  }
}
