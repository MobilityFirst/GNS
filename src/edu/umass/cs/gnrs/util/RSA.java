package edu.umass.cs.gnrs.util;

/*************************************************************************
 *  Compilation:  javac RSA.java
 *  Execution:    java RSA N
 *  
 *  Generate an N-bit public and private RSA key and use to encrypt
 *  and decrypt a random message.
 * 
 *  % java RSA 50
 *  public  = 65537
 *  private = 553699199426609
 *  modulus = 825641896390631
 *  message   = 48194775244950
 *  encrpyted = 321340212160104
 *  decrypted = 48194775244950
 *
 *  Known bugs (not addressed for simplicity)
 *  -----------------------------------------
 *  - It could be the case that the message >= modulus. To avoid, use
 *    a do-while loop to generate key until modulus happen to be exactly N bits.
 *
 *  - It's possible that gcd(phi, publicKey) != 1 in which case
 *    the key generation fails. This will only happen if phi is a
 *    multiple of 65537. To avoid, use a do-while loop to generate
 *    keys until the gcd is 1.
 *
 *************************************************************************/

import java.math.BigInteger;
import java.security.SecureRandom;
import java.security.*;


public class RSA {
	private final static BigInteger one      = new BigInteger("1");
	private final static SecureRandom random = new SecureRandom();

	private BigInteger privateKey;
	private BigInteger publicKey;
	private BigInteger modulus;
	int N;

	// generate an N-bit (roughly) public and private key
	public RSA(int N) {
		this.N = N;
		BigInteger p = BigInteger.probablePrime(N/2, random);
		BigInteger q = BigInteger.probablePrime(N/2, random);
		BigInteger phi = (p.subtract(one)).multiply(q.subtract(one));

		modulus    = p.multiply(q);                                  
		publicKey  = new BigInteger("65537");     // common value in practice = 2^16 + 1
		privateKey = publicKey.modInverse(phi);
	}


	BigInteger encrypt(BigInteger message) {
		return message.modPow(publicKey, modulus);
	}

	BigInteger decrypt(BigInteger encrypted) {
		return encrypted.modPow(privateKey, modulus);
	}

	public String toString() {
		String s = "";
		s += "public  = " + publicKey  + "\n";
		s += "private = " + privateKey + "\n";
		s += "modulus = " + modulus;
		return s;
	}

	/**
	 * Simulate a digital signature check for a 1000-bit message.
	 * @return
	 * @throws java.security.NoSuchAlgorithmException
	 * @throws java.io.UnsupportedEncodingException
	 */
	public boolean checkDigitalSignature() throws java.security.NoSuchAlgorithmException, java.io.UnsupportedEncodingException {


		BigInteger message = new BigInteger(N-1, random);
		//		// Take its SHA-1 hash.
		byte[] bytesOfMessage = message.toByteArray();
		//		System.out.println("Bytes" + bytesOfMessage.length);
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		md.update(bytesOfMessage);

		//		System.out.println("Length of digest in bytes:" + md.digest().length);

		// take a random digital signature, decrypt it.
		BigInteger digitalSignature = new BigInteger(800, random);  
		BigInteger decrypt = this.decrypt(digitalSignature);
		// We don't check whether Sha-1 has == decrypted digital signature. Always return true.
		return true;
	}

	public static void main(String[] args) {
		int N = Integer.parseInt("1000");
		RSA key = new RSA(N);
		System.out.println(key);
		long t1 = System.currentTimeMillis();
		int numberOfChecks = 100;

		for (int i = 0; i < numberOfChecks; i++) {
			try{
				key.checkDigitalSignature();
			}catch (Exception e) {
			}
		}
		long t2 = System.currentTimeMillis();

		System.out.println("Time to check a digital signature " + (t2 - t1)/numberOfChecks);


		// create random message, encrypt and decrypt
		//	BigInteger message = new BigInteger(N-1, random);

		//// create message by converting string to integer
		// String s = "test";
		// byte[] bytes = s.getBytes();
		// BigInteger message = new BigInteger(s);

		//	BigInteger encrypt = key.encrypt(message);
		//	BigInteger decrypt = key.decrypt(message);
		//	System.out.println("message   = " + message);
		//	System.out.println("encrpyted = " + encrypt);
		//	System.out.println("decrypted = " + decrypt);
	}

}