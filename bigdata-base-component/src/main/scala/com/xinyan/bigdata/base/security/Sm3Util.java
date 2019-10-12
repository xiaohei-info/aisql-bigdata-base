package com.xinyan.bigdata.base.security;


import com.xinyan.bigdata.base.security.sm.Hex;
import com.xinyan.bigdata.base.security.sm.SM3Digest;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

public class Sm3Util {
    public Sm3Util() {
    }

    public static String encrypt(String str) throws Exception {
        if(str != null && !"".equals(str)) {
            byte[] md = new byte[32];
            byte[] msg1 = str.getBytes();
            SM3Digest sm3 = new SM3Digest();
            sm3.update(msg1, 0, msg1.length);
            sm3.doFinal(md, 0);
            String ret = new String(Hex.encode(md));
            return ret;
        } else {
            return "";
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println(encrypt("sdfsfds"));
            System.out.println(encrypt("sdfsfds"));
        } catch (Exception var2) {
            ;
        }

    }
}
