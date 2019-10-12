package com.xinyan.bigdata.base.security;

import com.xinyan.bigdata.base.security.sm.SM4;
import com.xinyan.bigdata.base.security.sm.SM4_Context;
import com.xinyan.bigdata.base.security.sm.SMUtil;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

public class Sm4Util {
    private static boolean hexString = false;

    public Sm4Util() {
    }

    public static String encryptData_ECB(String secretKey, String plainText) {
        try {
            SM4_Context e = new SM4_Context();
            e.isPadding = true;
            e.mode = 1;
            byte[] keyBytes;
            if(hexString) {
                keyBytes = SMUtil.hexStringToBytes(secretKey);
            } else {
                keyBytes = secretKey.getBytes();
            }

            SM4 sm4 = new SM4();
            sm4.sm4_setkey_enc(e, keyBytes);
            byte[] encrypted = sm4.sm4_crypt_ecb(e, plainText.getBytes("UTF-8"));
            String cipherText = (new BASE64Encoder()).encode(encrypted);
            if(cipherText != null && cipherText.trim().length() > 0) {
                ;
            }

            return cipherText;
        } catch (Exception var7) {
            var7.printStackTrace();
            return null;
        }
    }

    public static String decryptData_ECB(String secretKey, String cipherText) {
        try {
            SM4_Context e = new SM4_Context();
            e.isPadding = true;
            e.mode = 0;
            byte[] keyBytes;
            if(hexString) {
                keyBytes = SMUtil.hexStringToBytes(secretKey);
            } else {
                keyBytes = secretKey.getBytes();
            }

            SM4 sm4 = new SM4();
            sm4.sm4_setkey_dec(e, keyBytes);
            byte[] decrypted = sm4.sm4_crypt_ecb(e, (new BASE64Decoder()).decodeBuffer(cipherText));
            return new String(decrypted, "UTF-8");
        } catch (Exception var6) {
            var6.printStackTrace();
            return null;
        }
    }

    public static String encryptData_CBC(String iv, String secretKey, String plainText) {
        try {
            SM4_Context e = new SM4_Context();
            e.isPadding = true;
            e.mode = 1;
            byte[] keyBytes;
            byte[] ivBytes;
            if(hexString) {
                keyBytes = SMUtil.hexStringToBytes(secretKey);
                ivBytes = SMUtil.hexStringToBytes(iv);
            } else {
                keyBytes = secretKey.getBytes();
                ivBytes = iv.getBytes();
            }

            SM4 sm4 = new SM4();
            sm4.sm4_setkey_enc(e, keyBytes);
            byte[] encrypted = sm4.sm4_crypt_cbc(e, ivBytes, plainText.getBytes("UTF-8"));
            String cipherText = (new BASE64Encoder()).encode(encrypted);
            if(cipherText != null && cipherText.trim().length() > 0) {
                ;
            }

            return cipherText;
        } catch (Exception var9) {
            var9.printStackTrace();
            return null;
        }
    }

    public static String decryptData_CBC(String iv, String secretKey, String cipherText) {
        try {
            SM4_Context e = new SM4_Context();
            e.isPadding = true;
            e.mode = 0;
            byte[] keyBytes;
            byte[] ivBytes;
            if(hexString) {
                keyBytes = SMUtil.hexStringToBytes(secretKey);
                ivBytes = SMUtil.hexStringToBytes(iv);
            } else {
                keyBytes = secretKey.getBytes();
                ivBytes = iv.getBytes();
            }

            SM4 sm4 = new SM4();
            sm4.sm4_setkey_dec(e, keyBytes);
            byte[] decrypted = sm4.sm4_crypt_cbc(e, ivBytes, (new BASE64Decoder()).decodeBuffer(cipherText));
            return new String(decrypted, "UTF-8");
        } catch (Exception var8) {
            var8.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        String plainText = "ererfeiisgod";
        String secretKey = "JeF8U9wHFOMfs2Y8";
        System.out.println("ECB模式");
        String cipherText = encryptData_ECB(secretKey, plainText);
        System.out.println("密文: " + cipherText);
        System.out.println("");
        plainText = decryptData_ECB(secretKey, cipherText);
        System.out.println("明文: " + plainText);
        System.out.println("");
        System.out.println("CBC模式");
        String iv = "UISwD9fW6cFh9SNS";
        cipherText = encryptData_CBC(iv, secretKey, plainText);
        System.out.println("密文: " + cipherText);
        System.out.println("");
        plainText = decryptData_CBC(iv, secretKey, cipherText);
        System.out.println("明文: " + plainText);
    }
}
