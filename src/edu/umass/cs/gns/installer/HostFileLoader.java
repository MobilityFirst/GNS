/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.installer;

import edu.umass.cs.gns.main.GNS;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
   * Reads a host file (hosts addresses one per line) and returns a list of hostname strings.
   *
   * @param hostsFile
   * @return a List of hostnames
   */
  public static List<String> loadHostFile(String hostsFile) throws FileNotFoundException {
    List<String> result = new ArrayList<String>();
    BufferedReader br = null;
    br = new BufferedReader(new FileReader(hostsFile));
    try {
      while (br.ready()) {
        String line = br.readLine();
        if (line == null || line.equals("") || line.equals(" ")) {
          continue;
        }
        result.add(line.trim());
      }
      br.close();
    } catch (IOException e) {
      GNS.getLogger().warning("Problem reading hosts file: " + e);
    }
    return result;
  }

}
