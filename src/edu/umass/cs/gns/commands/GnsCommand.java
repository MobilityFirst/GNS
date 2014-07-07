/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.clientsupport.CommandResponse;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import static edu.umass.cs.gns.httpserver.Defs.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class GnsCommand implements Comparable<GnsCommand> {

  protected CommandModule module;

  /**
   * Creates a new <code>ConsoleCommand</code> object
   *
   * @param module
   */
  public GnsCommand(CommandModule module) {
    this.module = module;
  }

  // We need to sort the commands to put the longer ones with the same command name first.
  @Override
  public int compareTo(GnsCommand c) {
    int alphaResult = getCommandName().compareTo(c.getCommandName());
    // sort by number of arguments putting the longer ones first because we need to do longest match first.
    if (alphaResult == 0) {
      int lengthDifference = getCommandParameters().length - c.getCommandParameters().length;
      if (lengthDifference != 0) {
        // longest should be "less than"
        return -(Integer.signum(lengthDifference));
      } else {
        // same length parameter strings just sort them alphabetically... they can't be equal
        return getCommandParametersString().compareTo(c.getCommandParametersString());
      }
    } else {
      return alphaResult;
    }
  }

  public abstract String[] getCommandParameters();

  public abstract String getCommandName();

  public abstract CommandResponse execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException;

  /**
   * Get the description of the command
   *
   * @return <code>String</code> of the command description
   */
  public abstract String getCommandDescription();

  /**
   * Get the usage of the command.
   *
   * @return <code>String</code> of the command usage ()
   */
  public String getUsage(boolean html) {
    if (html) {
      return "HTML Form: " + getHTMLForm() + NEWLINE
              + getCommandDescription();
    } else {
      return getTCPForm() + NEWLINE
              + getCommandDescription();
    }
  }

  public String getHTMLForm() {
    StringBuilder result = new StringBuilder();
    result.append(getCommandName());
    String[] parameters = getCommandParameters();
    String prefix = QUERYPREFIX;
    for (int i = 0; i < parameters.length; i++) {
      // special case to remove SIGNATUREFULLMESSAGE which isn't for HTML form
      if (!SIGNATUREFULLMESSAGE.equals(parameters[i])) {
        result.append(prefix);
        result.append(parameters[i]);
        result.append(VALSEP);
        result.append("<" + parameters[i] + ">");
        prefix = KEYSEP;
      }
    }
    return result.toString();
  }

  public String getTCPForm() {
    StringBuilder result = new StringBuilder();
    result.append("Command: ");
    result.append(getCommandName());
    String[] parameters = getCommandParameters();
    result.append(" Parameters: ");
    String prefix = "";
    for (int i = 0; i < parameters.length; i++) {
      if (!SIGNATUREFULLMESSAGE.equals(parameters[i])) {
        result.append(prefix);
        result.append(parameters[i]);
        prefix = ", ";
      }
    }
    return result.toString();
  }

  public String getCommandParametersString() {
    StringBuilder result = new StringBuilder();
    String[] parameters = getCommandParameters();
    String prefix = "";
    for (int i = 0; i < parameters.length; i++) {
      result.append(prefix);
      result.append(parameters[i]);
      prefix = ",";
    }
    return result.toString();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + " " + getCommandName() + " " + getCommandParametersString();
  }
}
