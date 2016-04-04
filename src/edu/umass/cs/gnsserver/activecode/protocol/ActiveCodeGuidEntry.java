package edu.umass.cs.gnsserver.activecode.protocol;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;

import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * @author gaozy
 * FIXME: this is a temporary fix to 
 */
public class ActiveCodeGuidEntry implements Serializable{
	public String entityName;
	public String guid;
	public PublicKey publicKey;
	public PrivateKey privateKey;
	
	public ActiveCodeGuidEntry(GuidEntry guidEntry){
		this.entityName = guidEntry.getEntityName();
		this.guid = guidEntry.getGuid();
		this.publicKey = guidEntry.getPublicKey();
		this.privateKey = guidEntry.getPrivateKey();
	}
	

}
