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
 *  Initial developer(s): Westy
 *
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
   * @param username
   * @param host
   * @param keyFile
   * @param withSudo
   * @param scriptName
   * @param command
   * @param arguments
   */
  public static void executeBashScript(String username, String host, File keyFile, boolean withSudo, String scriptName, String command, Object... arguments) {
    SSHClient.execWithSudoNoPass(username, host, keyFile, "echo \'" + command + "\' > " + scriptName);
    SSHClient.execWithSudoNoPass(username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    //SSHClient.execWithSudoNoPass(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString());
    //SSHClient.exec(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString(), withSudo, null);
    SSHClient.exec(username, host, keyFile, "bash " + scriptName + argumentList.toString(), withSudo, null);
  }
  
  /**
   *
   * @param username
   * @param host
   * @param keyFile
   * @param scriptName
   * @param command
   * @param arguments
   */
  public static void executeBashScriptWithSudoNoPass(String username, String host, File keyFile, String scriptName, String command, Object... arguments) {
    SSHClient.execWithSudoNoPass(username, host, keyFile, "echo \'" + command + "\' > " + scriptName);
    SSHClient.execWithSudoNoPass(username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    //SSHClient.execWithSudoNoPass(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString());
    SSHClient.execWithSudoNoPass(username, host, keyFile, "bash " + scriptName + argumentList.toString());
    //SSHClient.exec(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString(), withSudo, null);
  }

  /**
   *
   * @param username
   * @param host
   * @param keyFile
   * @param scriptName
   * @param command
   * @param arguments
   */
  public static void executeBashScriptNoSudo(String username, String host, File keyFile, String scriptName, String command, Object... arguments) {
    SSHClient.exec(username, host, keyFile, "echo \'" + command + "\' > " + scriptName);
    SSHClient.exec(username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    //SSHClient.exec(username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString());
    SSHClient.exec(username, host, keyFile, "bash " + scriptName + argumentList.toString());
  }

  /**
   * Write a script file and execute it.
   *
   * @param username
   * @param host
   * @param keyFile
   * @param scriptName
   * @param command
   * @param arguments
   */
  public static void executeBashScript(String username, String host, File keyFile, String scriptName, String command, Object... arguments) {
    executeBashScript(username, host, keyFile, false, scriptName, command, arguments);
  }

  /**
   *
   * @param arg
   */
  public static void main(String[] arg) {
    ExecuteBash.executeBashScript("ec2-user", "23.21.160.80",
            new File("/Users/westy/.ssh/aws.pem"),
            true,
            "test.sh",
            "#!/bin/bash\n"
            + "# make current directory the directory this script is in\n"
            + "DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"\n"
            + "cd $DIR\n"
            + "if [ -f testfile ]; then\n"
            + "mv --backup=numbered testfile testfile.save\n"
            + "fi\n"
            + "touch testfile\n");

  }
}
