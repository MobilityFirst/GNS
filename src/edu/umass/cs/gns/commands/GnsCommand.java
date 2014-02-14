/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.client.AccountAccess;
import edu.umass.cs.gns.client.FieldAccess;
import edu.umass.cs.gns.client.FieldMetaData;
import edu.umass.cs.gns.client.GroupAccess;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
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
  
  protected FieldAccess fieldAccess = FieldAccess.getInstance();
  protected AccountAccess accountAccess = AccountAccess.getInstance();
  protected FieldMetaData fieldMetaData = FieldMetaData.getInstance();
  //private GroupAccessV1 groupAccessV1 = GroupAccessV1.getInstance();
  protected GroupAccess groupAccess = GroupAccess.getInstance();

  /**
   * Creates a new <code>ConsoleCommand</code> object
   * 
   * @param reader the console reader we are attached to
   */
  public GnsCommand(CommandModule module) {
    this.module = module;
  }

  @Override
  public int compareTo(GnsCommand c) {
    int alphaResult = getCommandName().compareTo(c.getCommandName());
    // sort by number of arguments putting the longer ones first because we need to do longest match first.
    if (alphaResult == 0) {
      // longest should be "less than"
      return -(Integer.signum(getCommandParameters().length - c.getCommandParameters().length));
    } else {
      return alphaResult;
    }
  }

  public abstract String[] getCommandParameters();

  public abstract String getCommandName();

  public abstract String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException, JSONException, NoSuchAlgorithmException, SignatureException;

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
  public String getUsage() {
    String usage = "HTML Form: " + getHTMLForm() + NEWLINE 
            + getCommandDescription();
    return usage;
  }

  public String getHTMLForm() {
    StringBuilder result = new StringBuilder();
    result.append(getCommandName());
    String[] parameters = getCommandParameters();
    String prefix = QUERYPREFIX;
    for (int i = 0; i < parameters.length; i++) {
      result.append(prefix);
      result.append(parameters[i]);
      result.append(VALSEP);
      result.append("<" + parameters[i] + ">");
      prefix = KEYSEP;
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
