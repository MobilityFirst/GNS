package edu.umass.cs.gnsserver.activecode.prototype;

/**
 * @author gaozy
 *
 */
public class ActiveException extends Exception{

  private static final long serialVersionUID = 1L;
  
  public ActiveException(String msg){
	  super(msg);
  }
  
  public ActiveException(){
	  this("");
  }

}
