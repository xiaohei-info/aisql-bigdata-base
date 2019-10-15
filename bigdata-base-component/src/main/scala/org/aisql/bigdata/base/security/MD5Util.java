package org.aisql.bigdata.base.security;

import sun.misc.BASE64Encoder;

import java.security.MessageDigest;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */
public class MD5Util {
    private static final String ALGORITHM_MD5 = "MD5";
    private static final String UTF_8 = "UTF-8";

    public MD5Util() {
    }

    public static String encrypt32(String encryptStr) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] e = md5.digest(encryptStr.getBytes());
            StringBuffer hexValue = new StringBuffer();

            for(int i = 0; i < e.length; ++i) {
                int val = e[i] & 255;
                if(val < 16) {
                    hexValue.append("0");
                }

                hexValue.append(Integer.toHexString(val));
            }

            encryptStr = hexValue.toString();
            return encryptStr;
        } catch (Exception var6) {
            throw new java.lang.SecurityException("MD5加密失败", var6);
        }
    }

    @Deprecated
    public static String encrypt64(String encryptStr) {
        try {
            MessageDigest e = MessageDigest.getInstance("MD5");
            BASE64Encoder base64Encoder = new BASE64Encoder();
            encryptStr = base64Encoder.encode(e.digest(encryptStr.getBytes("UTF-8")));
            return encryptStr;
        } catch (Exception var3) {
            throw new java.lang.SecurityException("MD5加密失败", var3);
        }
    }

    public static String encrypt16(String encryptStr) {
        return encrypt32(encryptStr).substring(8, 24);
    }

    public static void main(String[] args) throws Exception {
        String encryptStr = "123456";
        System.out.println("MD5_16:" + encrypt16(encryptStr));
        System.out.println("MD5_32:" + encrypt32(encryptStr));
        System.out.println("MD5_64:" + encrypt64(encryptStr));
    }
}
