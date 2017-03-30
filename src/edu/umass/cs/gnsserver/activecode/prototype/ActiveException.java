package edu.umass.cs.gnsserver.activecode.prototype;

/**
 * This class is used to indicate that an exception happens during the code execution.
 * TODO: enumerate all possible exceptions for ActiveException
 * 
 * @author gaozy
 */
public class ActiveException extends Exception{

  private static final long serialVersionUID = 1L;
  
	/**
	 * The msg parameter is used to indicate the cause of the code execution failure.
	 * @param msg
	 */
	 public ActiveException(String msg){
		 super(msg);
	 }
  
  	/**
	 * An default constructor of ActiveException
	 */
	public ActiveException(){
		this("");
	}

}
