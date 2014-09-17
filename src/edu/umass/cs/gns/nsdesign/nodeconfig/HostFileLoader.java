/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.nodeconfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author westy
 */
public class HostFileLoader {

  /**
   * A tuple of NodeId and hostname.
   */
  public static class HostSpec {

    private final NodeId<String> id;
    private final String name;
    private final Integer startPort;

    public HostSpec(String id, String name, Integer startPort) {
      this.id = new NodeId(id);
      this.name = name;
      this.startPort = startPort;
    }

    public NodeId<String> getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public Integer getStartPort() {
      return startPort;
    }
    
  }

  /**
   * Reads a host file (hosts addresses one per line) and returns a list of HostSpec objects.
   * 
   * This currently supports three formats (one of these per line):
   * <code>
   * {hostname}
   * or
   * {number}{whitespace}{hostname}
   * or
   * {number}{whitespace}{hostname}{whitespace}{startingport}
   * </code>
   * Also, you can't mix formats in one file.
   *
   * @param hostsFile
   * @return a List of hostnames
   */
  public static List<HostSpec> loadHostFile(String hostsFile) throws Exception {
    List<HostSpec> result = new ArrayList<HostSpec>();
    BufferedReader br = new BufferedReader(new FileReader(hostsFile));
    try {
      int hostCnt = 0;
      boolean hasNumbers = false;
      while (br.ready()) {
        String line = br.readLine();
        if (line == null || line.equals("") || line.equals(" ")) {
          continue;
        }
        String[] tokens = line.split("\\s+");
        if (tokens.length > 1) {
          String id = tokens[0];
          Integer port = tokens.length > 2 ? Integer.parseInt(tokens[2]) : null;
          result.add(new HostSpec(id, tokens[1], port));
          hasNumbers = true;
        } else if (tokens.length == 1) {
          if (hasNumbers) {
            throw new IOException("Can't mix format with IDs provided and not provided:" + line);
          }
          result.add(new HostSpec(Integer.toString(hostCnt), tokens[0], null));
        } else {
          throw new IOException("Bad host format:" + line);
        }
      }
      br.close();
    } catch (IOException e) {
      throw new Exception("Problem reading hosts file: " + e);
    }
    return result;
  }


}
