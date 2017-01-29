
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.main.GNSConfig;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;


public class CommandModule {

  // Indicates if we're using the new command enums
  private static boolean useCommandEnums = true;

  private Map<CommandType, AbstractCommand> commandLookupTable;


  public void addCommand(CommandType commandType, AbstractCommand command) {
    if (commandLookupTable.get(commandType) != null) {
      GNSConfig.getLogger().log(Level.SEVERE,
              "Duplicate command: {0}", commandType);
    }
    commandLookupTable.put(commandType, command);
  }

  // Used only for generating the description of all commands
  private TreeSet<AbstractCommand> commands;
  //private boolean adminMode = false;


  public CommandModule() {
    initCommands();
  }

  private void initCommands() {
    commandLookupTable = new HashMap<>();
    // Used only for generating the description of all commands
    this.commands = new TreeSet<>();
    if (useCommandEnums) {
      addCommands(CommandType.getCommandClasses(), commands);
    } else {
      throw new UnsupportedOperationException("Old style command init has been deprecated.");
      //addCommands(CommandDefs.getCommandDefs(), commands);
    }
    ClientCommandProcessorConfig.getLogger().log(Level.FINE,
            "{0} commands added.", commands.size());
  }


  protected void addCommands(List<Class<?>> commandClasses, Set<AbstractCommand> commands) {
    for (int i = 0; i < commandClasses.size(); i++) {
      Class<?> clazz = commandClasses.get(i);
      AbstractCommand command = createCommandInstance(clazz);
      if (command != null) {
        commandLookupTable.put(command.getCommandType(), command);
        // Used only for generating the description of all commands
        commands.add(command);
      }
    }
  }


  protected void addCommands(Class<?>[] commandClasses, Set<AbstractCommand> commands) {
    for (int i = 0; i < commandClasses.length; i++) {
      Class<?> clazz = commandClasses[i];
      AbstractCommand command = createCommandInstance(clazz);
      if (command != null) {
        commandLookupTable.put(command.getCommandType(), command);
        // Legacy - used only for generating the description of all commands
        commands.add(command);
      }
    }
  }

  private AbstractCommand createCommandInstance(Class<?> clazz) {
    try {
      Constructor<?> constructor;
      try {
        constructor = clazz.getConstructor(new Class<?>[]{this.getClass()});
      } catch (NoSuchMethodException e) {
        constructor = clazz.getConstructor(new Class<?>[]{CommandModule.class});
      }
      AbstractCommand command = (AbstractCommand) constructor.newInstance(new Object[]{this});
      ClientCommandProcessorConfig.getLogger().log(Level.FINER,
              "Creating command {0}: {1} with {2}: {3}",
              new Object[]{command.getCommandType().getInt(), clazz.getCanonicalName(),
                command.getCommandType().toString(),
                command.getCommandParametersString()});
      return command;
    } catch (SecurityException | NoSuchMethodException |
            InstantiationException | IllegalAccessException |
            IllegalArgumentException | InvocationTargetException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
              "Unable to create command for class {0}: {1}",
              new Object[]{clazz.getCanonicalName(), e});
    }
    return null;
  }


  public AbstractCommand lookupCommand(CommandType commandType) {
    return commandLookupTable.get(commandType);
  }


  public AbstractCommand lookupCommand(String commandName) {
    try {
      return CommandModule.this.lookupCommand(CommandType.valueOf(commandName));
    } catch (IllegalArgumentException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
              "Unable to parse command name {0}", commandName);
      return null;
    }
  }


  public AbstractCommand lookupCommand(JSONObject json) {
    AbstractCommand command = null;
    if (json.has(GNSProtocol.COMMAND_INT.toString())) {
      try {
        command = CommandModule.this.lookupCommand(
                CommandType.getCommandType(json.getInt(GNSProtocol.COMMAND_INT.toString())));
        // Some sanity checks
        String commandName = json.optString(GNSProtocol.COMMANDNAME.toString(), null);
        // Check to see if command name is the same
        if (command != null && commandName != null
                && !commandName.equals(command.getCommandType().toString())) {
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                  "Command name {0} in json does not match {1}",
                  new Object[]{commandName, command.getCommandType().toString()});
          command = null;
        }
        if (command != null && !JSONContains(json, command.getCommandRequiredParameters())) {
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                  "For command {0} missing required parameter {1}",
                  new Object[]{command.getCommandType(), JSONMissing(json, command.getCommandRequiredParameters())});
          command = null;
        }
      } catch (JSONException e) {
        // do nothing
      }
    } else {
      ClientCommandProcessorConfig.getLogger().warning("No command int in command "
              + json.optString(GNSProtocol.COMMANDNAME.toString(), "also missing command name!"));
    }
    if (command != null) {
      ClientCommandProcessorConfig.getLogger().log(Level.FINEST,
              "Found {0} using table lookup", command);
      return command;
    }
    // Keep the old method for backward compatibility with older clients that
    // aren't using the GNSProtocol.COMMAND_INT.toString() field
    return lookupCommandFromCommandName(json);
  }


  public AbstractCommand lookupCommandFromCommandName(JSONObject json) {
    String action;
    try {
      action = json.getString(GNSProtocol.COMMANDNAME.toString());
    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
              "Unable to find " + GNSProtocol.COMMANDNAME.toString()
              + " key in JSON command: {0} : {1}", new Object[]{json, e});
      return null;
    }
    return CommandModule.this.lookupCommand(action);
  }


  public static final String STANDARD_PREAMBLE = "COMMAND PACKAGE: %s";


  public static final String WIKI_PREAMBLE = "{| class=\"wikitable\"\n"
          + "|+ Commands in %s\n"
          + "! scope=\"col\" | Command Name\n"
          + "! scope=\"col\" | Required Parameters\n"
          + "! scope=\"col\" | Optional Parameters\n"
          + "! scope=\"col\" | Description";


  public String allCommandDescriptions(CommandDescriptionFormat format) {
    StringBuilder result = new StringBuilder();
    List<AbstractCommand> commandList = new ArrayList<>(commands);
    // First sort by name
    Collections.sort(commandList, CommandNameComparator);
    // The sort them by package
    Collections.sort(commandList, CommandPackageComparator);
    String lastPackageName = null;
    for (AbstractCommand command : commandList) {
      String packageName = command.getClass().getPackage().getName();
      if (!packageName.equals(lastPackageName)) {
        if (format.equals(CommandDescriptionFormat.TCP_Wiki) && lastPackageName != null) {
          // finish last table
          result.append("|}");
        }
        lastPackageName = packageName;
        result.append(GNSProtocol.NEWLINE.toString());
        result.append(String.format(format.equals(CommandDescriptionFormat.TCP_Wiki)
                ? WIKI_PREAMBLE : STANDARD_PREAMBLE, lastPackageName));
        result.append(GNSProtocol.NEWLINE.toString());
      }
      //result.append(GNSProtocol.NEWLINE.toString());
      //result.append(cnt++ + ": ");
      result.append(command.getUsage(format));
      result.append(GNSProtocol.NEWLINE.toString());
      result.append(GNSProtocol.NEWLINE.toString());
    }
    return result.toString();
  }

  private String JSONMissing(JSONObject json, String[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (json.optString(parameters[i], null) == null) {
        return parameters[i];
      }
    }
    return null;
  }

  private boolean JSONContains(JSONObject json, String[] parameters) {
    return JSONMissing(json, parameters) == null;
  }

//
//  public void setAdminMode(boolean adminMode) {
//    this.adminMode = adminMode;
//  }
  private static Comparator<AbstractCommand> CommandPackageComparator
          = new Comparator<AbstractCommand>() {

    @Override
    public int compare(AbstractCommand command1, AbstractCommand command2) {

      String packageName1 = command1.getClass().getPackage().getName();
      String packageName2 = command2.getClass().getPackage().getName();

      //ascending order
      return packageName1.compareTo(packageName2);

      //descending order
      //return fruitName2.compareTo(fruitName1);
    }

  };


  private static Comparator<AbstractCommand> CommandNameComparator
          = new Comparator<AbstractCommand>() {

    @Override
    public int compare(AbstractCommand command1, AbstractCommand command2) {

      String commandName1 = command1.getCommandType().toString();
      String commandName2 = command2.getCommandType().toString();

      //ascending order
      return commandName1.compareTo(commandName2);

    }

  };
}
