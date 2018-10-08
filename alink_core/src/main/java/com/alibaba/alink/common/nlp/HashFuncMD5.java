
package com.alibaba.alink.common.nlp;

import java.math.BigInteger;
import java.security.MessageDigest;

public class HashFuncMD5 {
    public String message;
    public HashFuncMD5( String message ) {
        this.message = message;
    }
    public BigInteger hexdigest() throws Exception {
        MessageDigest md5 = MessageDigest.getInstance( "MD5" );
        md5.update( this.message.getBytes());
        BigInteger res = new BigInteger( 1, md5.digest() );
        return res;
    }
}
