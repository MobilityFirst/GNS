package edu.umass.cs.gnsclient.privacyclient.common;

import java.security.PublicKey;

/**
 * This class represents an ACL entry
 * @author adipc
 *
 */
public class ACLEntry 
{
	private final String guidString;
	private final PublicKey publicKey;
	
	public ACLEntry(String guidString, PublicKey publicKey)
	{
		this.guidString = guidString;
		this.publicKey = publicKey;
	}
	
	public String getGuidString()
	{
		return this.guidString;
	}
	
	public PublicKey getPublicKey()
	{
		return this.publicKey;
	}
}