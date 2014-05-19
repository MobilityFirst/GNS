package edu.umass.cs.gns.test;

/**
 * Different types of experiment we run via GNS.
 *
 * Created by abhigyan on 5/14/14.
 */
public enum ExpType {

  TRACE("trace"),               // sends requests given in trace
  CONNECT_TIME("connect_time"); // measures time to connect for a name

  private String expType;

  private ExpType(String expType){
    this.expType = expType;
  }

  public static ExpType getExpType(String expType) {
    if (expType.equals(TRACE.expType)) return TRACE;
    if (expType.equals(CONNECT_TIME.expType)) return CONNECT_TIME;
    throw new IllegalArgumentException();
  }
}
