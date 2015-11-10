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
package edu.umass.cs.gnsserver.nodeconfig;

import edu.umass.cs.gnsserver.main.GNS;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads a host file (hosts addresses one per line) and returns a list of HostSpec objects.
 *
 * @author westy
 */
public class HostFileLoader {

  private static final Long INVALID_FILE_VERSION = -1L;
  private static Long fileVersion = INVALID_FILE_VERSION;

  private static final boolean debuggingEnabled = false;

  private static boolean hostFileHasNodeIds = false;

  /**
   * Reads a host file (hosts addresses one per line) and returns a list of HostSpec objects.
   * The first line of the file can be a Long representing the file version.
   * This will be read and saved by this call.
   * To read that call <code>readVersionLine</code>.
   *
   * This currently supports these line formats (one of these per line):
   * <code>
   * {hostname}
   * or
   * {hostname}{whitespace}{ip Address} // mainly for LNS hosts files
   * or
   * {nodeId}{whitespace}{hostname}{whitespace}{startingport}
   * or
   * {nodeId}{whitespace}{hostname}{whitespace}{ip Address}
   * or
   * {nodeId}{whitespace}{hostname}{whitespace}{startingport}{whitespace}{ip Address}
   * </code>
   * Also, you can't mix the above line formats in one file.
   *
   * @param hostsFile
   * @return a List of hostnames
   * @throws java.lang.Exception
   */
  public static List<HostSpec> loadHostFile(String hostsFile) throws Exception {
    hostFileHasNodeIds = false;
    List<HostSpec> result = new ArrayList<HostSpec>();
    BufferedReader br = new BufferedReader(new FileReader(hostsFile));
    boolean readFirstLine = false;
    try {
      while (br.ready()) {
        String line = br.readLine();
        if (line == null || line.equals("") || line.equals(" ")
                || line.startsWith("#")) {
          // do nothing
        } else if (!readFirstLine && isLineTheFileVersion(line)) {
          fileVersion = getTheVersionFromLine(line);
          if (debuggingEnabled) {
            GNS.getLogger().info("Read version line: " + fileVersion);
          }
        } else {
          result.add(parseHostline(line));
        }
        readFirstLine = true;
      }
      br.close();
    } catch (IOException e) {
      throw new Exception("Problem reading hosts file: " + e);
    }
    return result;
  }

  /**
   * Parses a line from a host file.
   * 
   * Handles these cases:
   *
   * hostname
   * hostname ipAddressInDotNotation
   * id hostname port
   * id hostname ipAddressInDotNotation
   * id hostname port ipAddressInDotNotation
   *
   * @param line
   * @return a {@link HostSpec}
   * @throws IOException
   */
  private static HostSpec parseHostline(String line) throws IOException {
    String[] tokens = line.split("\\s+");
    if (tokens.length > 1) {
      // hostname ipAddressInDotNotation (FOR LNS ONLY)
      if (tokens.length == 2 && tokens[1].contains(".")) {
        return new HostSpec(tokens[0], tokens[0], tokens[1], null);
        // id hostname port
        // id hostname ipAddressInDotNotation
        // id hostname port ipAddressInDotNotation
      } else {
        String idString = tokens[0];
        String externalIP = null;
        Integer port = null;
        if (tokens.length > 2) {
          if (tokens[2].contains(".")) {
            externalIP = tokens[2];
            port = null;
          } else {
            port = Integer.parseInt(tokens[2]);
          }
        }
        if (tokens.length > 3) {
          if (port == null) {
            throw new IOException("Bad port spec on line " + line + ": " + tokens[2]);
          }
          externalIP = tokens[3];
        }
        hostFileHasNodeIds = true;
        // Parse as an Integer if we can otherwise a String.
//        Object nodeID = -1;
//        try {
//          nodeID = Integer.parseInt(idString);
//        } catch (NumberFormatException e) {
          String nodeID = idString;
        //}
        if (debuggingEnabled) {
          GNS.getLogger().info("Read ID: " + nodeID);
        }
//        if (tokens[1].contains(".") && !tokens[1].equals("127.0.0.1")) {
//          throw new IOException("Bad hostname " + tokens[1] + ". Should not be an ip address.");
//        }
        return new HostSpec(nodeID, tokens[1], externalIP, port);
      }
      // hostname
    } else if (tokens.length == 1) {
      if (hostFileHasNodeIds) {
        throw new IOException("Can't mix format with IDs provided and not provided:" + line);
      }
      return new HostSpec(tokens[0], tokens[0], null, null);
    } else {
      throw new IOException("Bad host format:" + line);
    }
  }

  /**
   * Reads the version form the first line of the host file.
   * Returns null if it doesn't exist.
   *
   * @param hostsFile
   * @return a long
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static Long readVersionLine(String hostsFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(hostsFile));
    String line = br.readLine();
    br.close();
    return getTheVersionFromLine(line);
  }

  /**
   * Returns true if the node file changed versions.
   * 
   * @param hostsFile
   * @return true if the node file changed versions
   * @throws IOException
   */
  public static boolean isChangedFileVersion(String hostsFile) throws IOException {
    Long newVersion = readVersionLine(hostsFile);
    if (debuggingEnabled) {
      GNS.getLogger().info("Old version: " + fileVersion + " new version: " + newVersion);
    }
    return newVersion != null && newVersion != INVALID_FILE_VERSION && newVersion != fileVersion;
  }

  private static Long getTheVersionFromLine(String line) {
    Long version = null;
    String[] tokens = line.split("\\s+");
    // check to see if it's a host spec on the first line
    if (tokens.length == 1) {
      try {
        version = Long.parseLong(line);
      } catch (NumberFormatException e) {
      }
    }
    return version;
  }

  private static boolean isLineTheFileVersion(String line) {
    return getTheVersionFromLine(line) != null;
  }

  /**
   * Returns the file version.
   * 
   * @return a long
   */
  public static Long getFileVersion() {
    return fileVersion;
  }

}
