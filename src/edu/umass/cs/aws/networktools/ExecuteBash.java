/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.aws.networktools;

import java.io.File;

/**
 *
 * @author westy
 */
public class ExecuteBash {
  
  private static final String CHMODCOMMAND = "chmod ugo+x";
  private static final String FILESEPARATOR = System.getProperty("file.separator");
 
  /**
   * Write a script file and execute it.
   * 
   * @param host
   * @param keyFile
   * @param withSudo
   * @param scriptName
   * @param command
   * @param arguments 
   */
  public static void executeBashScript(String username, String host, File keyFile, boolean withSudo, String scriptName, String command, Object... arguments) {
    SSHClient.execWithSudoNoPass(username, host, keyFile, "echo \"" + command + "\" > " + scriptName);
    SSHClient.execWithSudoNoPass(username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    SSHClient.exec(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString(), withSudo, null);
  }
  
  public static void executeBashScriptNoSudo(String username, String host, File keyFile, String scriptName, String command, Object... arguments) {
    SSHClient.exec(username, host, keyFile, "echo \"" + command + "\" > " + scriptName);
    SSHClient.exec(username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    SSHClient.exec(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString());
  }

  /**
   * Write a script file and execute it.
   *
   * @param host
   * @param keyFile
   */
  public static void executeBashScript(String username, String host, File keyFile, String scriptName, String command, Object... arguments) {
    executeBashScript(username, host, keyFile, false, scriptName, command, arguments);
  }

}
