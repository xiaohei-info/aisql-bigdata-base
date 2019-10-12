package com.xinyan.bigdata.base.security.sm;


/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */
public class SM3 {
    public static final byte[] iv = new byte[]{(byte)115, (byte)-128, (byte)22, (byte)111, (byte)73, (byte)20, (byte)-78, (byte)-71, (byte)23, (byte)36, (byte)66, (byte)-41, (byte)-38, (byte)-118, (byte)6, (byte)0, (byte)-87, (byte)111, (byte)48, (byte)-68, (byte)22, (byte)49, (byte)56, (byte)-86, (byte)-29, (byte)-115, (byte)-18, (byte)77, (byte)-80, (byte)-5, (byte)14, (byte)78};
    public static int[] Tj = new int[64];

    public SM3() {
    }

    public static byte[] CF(byte[] V, byte[] B) {
        int[] v = convert(V);
        int[] b = convert(B);
        return convert(CF(v, b));
    }

    private static int[] convert(byte[] arr) {
        int[] out = new int[arr.length / 4];
        byte[] tmp = new byte[4];

        for(int i = 0; i < arr.length; i += 4) {
            System.arraycopy(arr, i, tmp, 0, 4);
            out[i / 4] = bigEndianByteToInt(tmp);
        }

        return out;
    }

    private static byte[] convert(int[] arr) {
        byte[] out = new byte[arr.length * 4];
        Object tmp = null;

        for(int i = 0; i < arr.length; ++i) {
            byte[] var4 = bigEndianIntToByte(arr[i]);
            System.arraycopy(var4, 0, out, i * 4, 4);
        }

        return out;
    }

    public static int[] CF(int[] V, int[] B) {
        int a = V[0];
        int b = V[1];
        int c = V[2];
        int d = V[3];
        int e = V[4];
        int f = V[5];
        int g = V[6];
        int h = V[7];
        int[][] arr = expand(B);
        int[] w = arr[0];
        int[] w1 = arr[1];

        for(int out = 0; out < 64; ++out) {
            int ss1 = bitCycleLeft(a, 12) + e + bitCycleLeft(Tj[out], out);
            ss1 = bitCycleLeft(ss1, 7);
            int ss2 = ss1 ^ bitCycleLeft(a, 12);
            int tt1 = FFj(a, b, c, out) + d + ss2 + w1[out];
            int tt2 = GGj(e, f, g, out) + h + ss1 + w[out];
            d = c;
            c = bitCycleLeft(b, 9);
            b = a;
            a = tt1;
            h = g;
            g = bitCycleLeft(f, 19);
            f = e;
            e = P0(tt2);
        }

        int[] var18 = new int[]{a ^ V[0], b ^ V[1], c ^ V[2], d ^ V[3], e ^ V[4], f ^ V[5], g ^ V[6], h ^ V[7]};
        return var18;
    }

    private static int[][] expand(int[] B) {
        int[] W = new int[68];
        int[] W1 = new int[64];

        int arr;
        for(arr = 0; arr < B.length; ++arr) {
            W[arr] = B[arr];
        }

        for(arr = 16; arr < 68; ++arr) {
            W[arr] = P1(W[arr - 16] ^ W[arr - 9] ^ bitCycleLeft(W[arr - 3], 15)) ^ bitCycleLeft(W[arr - 13], 7) ^ W[arr - 6];
        }

        for(arr = 0; arr < 64; ++arr) {
            W1[arr] = W[arr] ^ W[arr + 4];
        }

        int[][] var4 = new int[][]{W, W1};
        return var4;
    }

    private static byte[] bigEndianIntToByte(int num) {
        return back(SMUtil.intToBytes(num));
    }

    private static int bigEndianByteToInt(byte[] bytes) {
        return SMUtil.byteToInt(back(bytes));
    }

    private static int FFj(int X, int Y, int Z, int j) {
        return j >= 0 && j <= 15?FF1j(X, Y, Z):FF2j(X, Y, Z);
    }

    private static int GGj(int X, int Y, int Z, int j) {
        return j >= 0 && j <= 15?GG1j(X, Y, Z):GG2j(X, Y, Z);
    }

    private static int FF1j(int X, int Y, int Z) {
        int tmp = X ^ Y ^ Z;
        return tmp;
    }

    private static int FF2j(int X, int Y, int Z) {
        int tmp = X & Y | X & Z | Y & Z;
        return tmp;
    }

    private static int GG1j(int X, int Y, int Z) {
        int tmp = X ^ Y ^ Z;
        return tmp;
    }

    private static int GG2j(int X, int Y, int Z) {
        int tmp = X & Y | ~X & Z;
        return tmp;
    }

    private static int P0(int X) {
        int y = rotateLeft(X, 9);
        y = bitCycleLeft(X, 9);
        int z = rotateLeft(X, 17);
        z = bitCycleLeft(X, 17);
        int t = X ^ y ^ z;
        return t;
    }

    private static int P1(int X) {
        int t = X ^ bitCycleLeft(X, 15) ^ bitCycleLeft(X, 23);
        return t;
    }

    public static byte[] padding(byte[] in, int bLen) {
        int k = 448 - (8 * in.length + 1) % 512;
        if(k < 0) {
            k = 960 - (8 * in.length + 1) % 512;
        }

        ++k;
        byte[] padd = new byte[k / 8];
        padd[0] = -128;
        long n = (long)(in.length * 8 + bLen * 512);
        byte[] out = new byte[in.length + k / 8 + 8];
        byte pos = 0;
        System.arraycopy(in, 0, out, 0, in.length);
        int var9 = pos + in.length;
        System.arraycopy(padd, 0, out, var9, padd.length);
        var9 += padd.length;
        byte[] tmp = back(SMUtil.longToBytes(n));
        System.arraycopy(tmp, 0, out, var9, tmp.length);
        return out;
    }

    private static byte[] back(byte[] in) {
        byte[] out = new byte[in.length];

        for(int i = 0; i < out.length; ++i) {
            out[i] = in[out.length - i - 1];
        }

        return out;
    }

    public static int rotateLeft(int x, int n) {
        return x << n | x >> 32 - n;
    }

    private static int bitCycleLeft(int n, int bitLen) {
        bitLen %= 32;
        byte[] tmp = bigEndianIntToByte(n);
        int byteLen = bitLen / 8;
        int len = bitLen % 8;
        if(byteLen > 0) {
            tmp = byteCycleLeft(tmp, byteLen);
        }

        if(len > 0) {
            tmp = bitSmall8CycleLeft(tmp, len);
        }

        return bigEndianByteToInt(tmp);
    }

    private static byte[] bitSmall8CycleLeft(byte[] in, int len) {
        byte[] tmp = new byte[in.length];

        for(int i = 0; i < tmp.length; ++i) {
            byte t1 = (byte)((in[i] & 255) << len);
            byte t2 = (byte)((in[(i + 1) % tmp.length] & 255) >> 8 - len);
            byte t3 = (byte)(t1 | t2);
            tmp[i] = (byte)t3;
        }

        return tmp;
    }

    private static byte[] byteCycleLeft(byte[] in, int byteLen) {
        byte[] tmp = new byte[in.length];
        System.arraycopy(in, byteLen, tmp, 0, in.length - byteLen);
        System.arraycopy(in, 0, tmp, in.length - byteLen, byteLen);
        return tmp;
    }

    static {
        int i;
        for(i = 0; i < 16; ++i) {
            Tj[i] = 2043430169;
        }

        for(i = 16; i < 64; ++i) {
            Tj[i] = 2055708042;
        }

    }
}
