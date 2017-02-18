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
package edu.umass.cs.gnsclient.console;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.prefs.Preferences;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.SimpleCompletor;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.console.commands.ConsoleCommand;
import edu.umass.cs.gnsclient.console.commands.Connect;
import edu.umass.cs.gnsclient.console.commands.GuidUse;
import edu.umass.cs.gnsclient.console.commands.Help;
import edu.umass.cs.gnsclient.console.commands.History;
import edu.umass.cs.gnsclient.console.commands.Quit;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.logging.Level;

import org.json.JSONException;

/**
 * This class defines a ConsoleModule
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class ConsoleModule {

  private final ConsoleReader console;
  private final TreeSet<ConsoleCommand> commands;
  private boolean quit = false;
  private boolean useGnsDefaults = true;

  /**
   *
   */
  private Completor consoleCompletor;
  private String promptString = CONSOLE_PROMPT + "not connected>";
  private GNSClientCommands gnsClient;
  private GuidEntry currentGuid;
  // might be a better way to do this, but for now
  private boolean accountVerified;
  private boolean silent;

  /**
   * Prompt to prepend to the console message prompts
   */
  public static final String CONSOLE_PROMPT = "GNS CLI - ";
  /**
   * location of the default command lists for the console modules
   */
  public static String DEFAULT_COMMAND_PROPERTIES_FILE = "edu/umass/cs/gnsclient/console/console.properties";

  /**
   * Creates a new <code>ConsoleModule.java</code> object
   *
   * @param console to refer from
   */
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

  /**
   * Retrieve the command history
   *
   * @return a List including the command history
   */
  @SuppressWarnings("unchecked")
  public List<String> getHistory() {
    return console.getHistory().getHistoryList();
  }

  private final static int NUMBER_OF_HISTORY_ITEMS_TO_STORE = 20;

  /**
   * Store the current command history
   */
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

  /**
   * Loads the commands for this module
   */
  protected final void loadCommands() {
    commands.clear();
    String commandClassesAsString = loadCommandsFromProperties("main");
    String[] commandClasses = parseCommands(commandClassesAsString);
    addCommands(commandClasses, commands);
  }

  /**
   * Parses a String representing a list of command classes (separated by
   * commas) and returns an String[] representing the command classes
   *
   * @param commandClassesAsString a String representing a list of command
   * classes (separated by commas)
   * @return a (eventually empty) String[] where each String represents a
   * command class
   */
  protected String[] parseCommands(String commandClassesAsString) {
    if (commandClassesAsString == null) {
      return new String[0];
    }
    String[] cmds = commandClassesAsString.split("\\s*,\\s*"); //$NON-NLS-1$
    return cmds;
  }

  /**
   * Add commands to this module. Commands instances are created by reflection
   * based on the command class names passed in parameter
   *
   * @param commandClasses a String[] containing the class names of the command
   * to instantiate
   * @param commands Set where the commands are added
   */
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

  /**
   * Extracts the commands from the command properties file as a single
   * <code>String</code> containing a list of comma-separated command classes.
   *
   * @param moduleID module ID used as the key in the properties file
   * @return a single <code>String</code> containing a list of comma-separated
   * command classes corresponding to the module identified by
   */
  protected String loadCommandsFromProperties(String moduleID) {

    System.out.println(this.getClass().getClassLoader().getResource(".").getPath());
    Properties props = new Properties();
    try {
      String propertiesFile = System.getProperty("console.commands", DEFAULT_COMMAND_PROPERTIES_FILE);
      GNSClientConfig.getLogger().log(Level.INFO, "Loading commands from {0}", propertiesFile);
      // Fixme: this fails (inputStream is null) when we try to run this from junit
      InputStream inputStream = ClassLoader.getSystemResourceAsStream(propertiesFile);
      if (inputStream == null) {
        GNSClientConfig.getLogger().log(Level.WARNING, "Unable to load from {0}", propertiesFile);
        return null;
      }
      props.load(inputStream);
    } catch (IOException e) {
      // fail silently: no commands will be loaded
    } catch (RuntimeException e) {
      GNSClientConfig.getLogger().log(Level.INFO, "Unable to load commands: {0}", e.getMessage());
      e.printStackTrace();
    }
    String commandClassesAsString = props.getProperty(moduleID, "");
    return commandClassesAsString;
  }

  /**
   * Loads the commands for this module
   */
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

  /**
   * Reload the completor associated with this module. This method must be
   * called if the list of commands has been dynamically modified.
   */
  protected synchronized void reloadCompletor() {
    console.removeCompletor(consoleCompletor);
    loadCompletor();
    console.addCompletor(consoleCompletor);
  }

  /**
   * Display help for this module
   *
   */
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

  /**
   * Quit this module
   */
  public void quit() {
    quit = true;
    console.removeCompletor(getCompletor());
  }

  /**
   * Get the prompt string for this module
   *
   * @return <code>String</code> to place before prompt
   */
  public String getPromptString() {
    if (silent) {
      return "";
    }
    return promptString;
  }

  /**
   * @param promptString the promptString to set
   */
  public void setPromptString(String promptString) {
    this.promptString = promptString;
  }

  /**
   * Handle a series of commands
   *
   */
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

      Map<String, ConsoleCommand> hashCommands = getHashCommands();
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
      } catch (UnknownCommandException e) {
        printString(e.getMessage() + "\n");
      } catch (Exception e) {
        printString("Error during console command execution: " + e.getMessage() + "\n");
        e.printStackTrace();
      }
    }
    GNSClientConfig.getLogger().fine("Quitting");
  }

  /**
   * Connect and try to find default GNSProtocol.GUID.toString() if defined.
   */
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
  private final List<Byte> currentLine = new ArrayList<>();

  /**
   * Implements SEQUOIA-887. We would like to create a BufferedReader to use its
   * readLine() method but can't because its eager cache would steal bytes from
   * JLine and drop them when we return, so painfully implement here our own
   * readLine() method, tyring to be bug for bug compatible with JLine.
   * <p>
   * This is a quite ugly hack. Among others we cannot use any read buffering
   * since consoleReader is exported and might be used elsewhere. At the very
   * least we would like to encapsulate the consoleReader so we can avoid
   * creating one in non-JLine mode. Re-open SEQUOIA-887 for such a proper fix.
   */
  private String readLineBypassJLine() throws IOException {
    // If JLine implements any kind of internal read buffering, we
    // are screwed.
    InputStream jlineInternal = console.getInput();

    /*
     * Unfortunately we can't do this because InputStreamReader returns -1/EOF
     * after every line!? So we have to decode bytes->characters by ourselves,
     * see below. Because of this we will FAIL with exotic locales, see
     * SEQUOIA-911
     */
    // Reader jlineInternal = new InputStreamReader(consoleReader.getInput());
    currentLine.clear();

    int ch = jlineInternal.read();

    if (ch == -1 /* EOF */ || ch == 4 /* ASCII EOT */) {
      return null;
    }

    /**
     * @see java.io.BufferedReader#readLine(boolean)
     * @see java.io.DataInputStream#readLine() and also the less elaborate JLine
     * keybinding.properties
     */
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

    /**
     * This String ctor is using the "default" java.nio.Charset encoding which
     * is locale-dependent; a Good Thing.
     */
    String line = new String(encoded);

    return line;
  }

  /**
   * Get the list of commands as strings for this module
   *
   * @return <code>Hashtable</code> list of <code>String</code> objects
   */
  public final Map<String, ConsoleCommand> getHashCommands() {
    HashMap<String, ConsoleCommand> hashCommands = new HashMap<>();
    for (ConsoleCommand consoleCommand : commands) {
      hashCommands.put(consoleCommand.getCommandName(), consoleCommand);
    }
    return hashCommands;
  }

  /**
   * Handle module command
   *
   * @param commandLine the command line to handle
   * @param hashCommands the list of commands available for this module
   * @throws Exception if fails *
   */
  public final void handleCommandLine(String commandLine, Map<String, ConsoleCommand> hashCommands)
          throws Exception {
    ConsoleCommand command = findConsoleCommand(commandLine, hashCommands);
    GNSClientConfig.getLogger().log(Level.FINE, "Command:{0}", command);
    if (command != null) {
      command.execute(commandLine.substring(command.getCommandName().length()));
      return;
    }
    throw new UnknownCommandException("Command " + commandLine + " is not supported here");
  }

  /**
   * Find the <code>ConsoleCommand</code> based on the name of the command from
   * the <code>commandLine</code> in the <code>hashCommands</code>. If more than
   * one <code>ConsoleCommand</code>'s command name start the same way, return
   * the <code>ConsoleCommand</code> with the longest one.
   *
   * @param commandLine the command line to handle
   * @param hashCommands the list of commands available for this module
   * @return the <code>ConsoleCommand</code> corresponding to the name of the
   * command from the <code>commandLine</code> or <code>null</code> if
   * there is no matching
   */
  public ConsoleCommand findConsoleCommand(String commandLine, Map<String, ConsoleCommand> hashCommands) {
    ConsoleCommand foundCommand = null;
    for (Map.Entry<String, ConsoleCommand> commandEntry : hashCommands.entrySet()) {
      String commandName = commandEntry.getKey();
      if (commandLine.startsWith(commandName)) {
        ConsoleCommand command = commandEntry.getValue();
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

  /**
   * Get access to the console
   *
   * @return <code>Console</code> instance
   */
  public ConsoleReader getConsole() {
    return console;
  }

  /**
   * Returns the console completor to use for this module.
   *
   * @return <code>Completor</code> object.
   */
  public Completor getCompletor() {
    return consoleCompletor;
  }

  /**
   * Returns the silent value.
   *
   * @return Returns the silent.
   */
  public boolean isSilent() {
    return silent;
  }

  /**
   * Toggle the console silent output on/off
   *
   * @param silent true if the output should be silent
   */
  public void setSilent(boolean silent) {
    this.silent = silent;
  }

  /**
   * Returns the gnsClient value.
   *
   * @return Returns the gnsClient.
   */
  public GNSClientCommands getGnsClient() {
    return gnsClient;
  }

  /**
   * Set the GNS client connection
   *
   * @param gnsClient A valid GNS connection
   */
  public void setGnsClient(GNSClientCommands gnsClient) {
    this.gnsClient = gnsClient;
  }

  /**
   * Returns the currentGuid value.
   *
   * @return Returns the currentGuid.
   */
  public GuidEntry getCurrentGuid() {
    return currentGuid;
  }

  /**
   * Returns the verified status of the current GNSProtocol.GUID.toString().
   *
   * @return true if the account is verified
   */
  public boolean isAccountVerified() {
    return accountVerified;
  }

  /**
   * Sets the account status to verified
   *
   * @param accountVerified true if the account status is verified
   */
  public void setAccountVerified(boolean accountVerified) {
    this.accountVerified = accountVerified;
  }

  /**
   * Sets the currentGuid value.
   *
   * @param guid The currentGuid to set.
   */
  public void setCurrentGuid(GuidEntry guid) {
    this.currentGuid = guid;
  }

  /**
   * Returns true if the current GNSProtocol.GUID.toString() is set and the account has been verified
   *
   * @return true if the current GNSProtocol.GUID.toString() is set and the account has been verified
   */
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

  /**
   * Sets the current GNSProtocol.GUID.toString() with the provided GNSProtocol.GUID.toString() and checks if the account is
   * verified.
   *
   * @param guid the GNSProtocol.GUID.toString() to use as current GNSProtocol.GUID.toString()
   */
  public void setCurrentGuidAndCheckForVerified(GuidEntry guid) {
    this.currentGuid = guid;
    if (currentGuid != null && gnsClient != null) {
      accountVerified = checkGnsIsAccountVerified(currentGuid);
    }
    if (!accountVerified && currentGuid != null) {
      printString(currentGuid.getEntityName() + " is not verified.\n");
    }
  }

  /**
   * Sets the guid as default GNSProtocol.GUID.toString() in user preferences and checks if the account
   * is verified.
   *
   * @param guid the GNSProtocol.GUID.toString() to set as default GNSProtocol.GUID.toString()
   */
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

  /**
   * Return the GNS host and port number in a string like "server.gns.name:8080"
   *
   * @return GNS host and port
   */
  public String getGnsInstance() {
    if (gnsClient == null) {
      return null;
    } else {
      return gnsClient.getGNSProvider();
    }
  }

  /**
   * Check if the given GNSProtocol.GUID.toString() is verified in the GNS.
   *
   * @param guid the GNSProtocol.GUID.toString() to lookup
   * @return true if the account is verified
   */
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

  /**
   * Outputs a string on the console output (without carriage return)
   *
   * @param string the string to print
   */
  public void printString(String string) {
    try {
      console.printString(string);
    } catch (IOException e) {
      GNSClientConfig.getLogger().warning("Problem printing string to console: " + e);
    }
  }

  /**
   * This class defines a CommandDelimiter used to delimit a command from user
   * input
   */
  private class CommandDelimiter extends ArgumentCompletor.AbstractArgumentDelimiter {

    /**
     * @see jline.ArgumentCompletor.AbstractArgumentDelimiter#isDelimiterChar(java.lang.String,
     * int)
     */
    @Override
    public boolean isDelimiterChar(String buffer, int pos) {
      String tentativeCmd = buffer.substring(0, pos);
      return isACompleteCommand(tentativeCmd);
    }

    /**
     * Test if the String input by the user insofar is a complete command or
     * not.
     *
     * @param input Text input by the user
     * @return <code>true</code> if the text input by the user is a complete
     * command name, <code>false</code> else
     */
    private boolean isACompleteCommand(String input) {
      boolean foundCompleteCommand = false;
      for (ConsoleCommand command : commands) {
        if (input.equals(command.getCommandName())) {
          foundCompleteCommand = !otherCommandsStartWith(command.getCommandName());
        }
      }
      return foundCompleteCommand;
    }

    private boolean otherCommandsStartWith(String commandName) {
      for (ConsoleCommand command : commands) {
        if (command.getCommandName().startsWith(commandName) && !command.getCommandName().equals(commandName)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   *
   * @return true if use defaults is true
   */
  public boolean isUseGnsDefaults() {
    return useGnsDefaults;
  }

  /**
   *
   * @param useGnsDefaults
   */
  public void setUseGnsDefaults(boolean useGnsDefaults) {
    this.useGnsDefaults = useGnsDefaults;
  }
}
