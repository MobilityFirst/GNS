/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.commands;

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
public abstract class NSCommand implements Comparable<NSCommand> {

  protected NSCommandModule module;

  /**
   * Creates a new <code>ConsoleCommand</code> object
   * 
   * @param module
   */
  public NSCommand(NSCommandModule module) {
    this.module = module;
  }

  // We need to sort the commands to put the longer ones with the same command name first.
  @Override
  public int compareTo(NSCommand c) {
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

  public abstract String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException;

  /**
   * Get the description of the command
   * 
   * @return <code>String</code> of the command description
   */
  public abstract String getCommandDescription();

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
