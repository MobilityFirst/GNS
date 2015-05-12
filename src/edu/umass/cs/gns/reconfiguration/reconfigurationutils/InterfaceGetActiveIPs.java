/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.ArrayList;

/**
 * Get active IP addresses. Used by the various demand profiles.
 * 
 * @author westy
 */
public interface InterfaceGetActiveIPs {
  
  public ArrayList<InetAddress> getActiveIPs();
  
}
