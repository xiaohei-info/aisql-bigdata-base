package com.xinyan.bigdata.base.security.sm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

public class SM4 {
    public static final int SM4_ENCRYPT = 1;
    public static final int SM4_DECRYPT = 0;
    public static final byte[] SboxTable = new byte[]{(byte)-42, (byte)-112, (byte)-23, (byte)-2, (byte)-52, (byte)-31, (byte)61, (byte)-73, (byte)22, (byte)-74, (byte)20, (byte)-62, (byte)40, (byte)-5, (byte)44, (byte)5, (byte)43, (byte)103, (byte)-102, (byte)118, (byte)42, (byte)-66, (byte)4, (byte)-61, (byte)-86, (byte)68, (byte)19, (byte)38, (byte)73, (byte)-122, (byte)6, (byte)-103, (byte)-100, (byte)66, (byte)80, (byte)-12, (byte)-111, (byte)-17, (byte)-104, (byte)122, (byte)51, (byte)84, (byte)11, (byte)67, (byte)-19, (byte)-49, (byte)-84, (byte)98, (byte)-28, (byte)-77, (byte)28, (byte)-87, (byte)-55, (byte)8, (byte)-24, (byte)-107, (byte)-128, (byte)-33, (byte)-108, (byte)-6, (byte)117, (byte)-113, (byte)63, (byte)-90, (byte)71, (byte)7, (byte)-89, (byte)-4, (byte)-13, (byte)115, (byte)23, (byte)-70, (byte)-125, (byte)89, (byte)60, (byte)25, (byte)-26, (byte)-123, (byte)79, (byte)-88, (byte)104, (byte)107, (byte)-127, (byte)-78, (byte)113, (byte)100, (byte)-38, (byte)-117, (byte)-8, (byte)-21, (byte)15, (byte)75, (byte)112, (byte)86, (byte)-99, (byte)53, (byte)30, (byte)36, (byte)14, (byte)94, (byte)99, (byte)88, (byte)-47, (byte)-94, (byte)37, (byte)34, (byte)124, (byte)59, (byte)1, (byte)33, (byte)120, (byte)-121, (byte)-44, (byte)0, (byte)70, (byte)87, (byte)-97, (byte)-45, (byte)39, (byte)82, (byte)76, (byte)54, (byte)2, (byte)-25, (byte)-96, (byte)-60, (byte)-56, (byte)-98, (byte)-22, (byte)-65, (byte)-118, (byte)-46, (byte)64, (byte)-57, (byte)56, (byte)-75, (byte)-93, (byte)-9, (byte)-14, (byte)-50, (byte)-7, (byte)97, (byte)21, (byte)-95, (byte)-32, (byte)-82, (byte)93, (byte)-92, (byte)-101, (byte)52, (byte)26, (byte)85, (byte)-83, (byte)-109, (byte)50, (byte)48, (byte)-11, (byte)-116, (byte)-79, (byte)-29, (byte)29, (byte)-10, (byte)-30, (byte)46, (byte)-126, (byte)102, (byte)-54, (byte)96, (byte)-64, (byte)41, (byte)35, (byte)-85, (byte)13, (byte)83, (byte)78, (byte)111, (byte)-43, (byte)-37, (byte)55, (byte)69, (byte)-34, (byte)-3, (byte)-114, (byte)47, (byte)3, (byte)-1, (byte)106, (byte)114, (byte)109, (byte)108, (byte)91, (byte)81, (byte)-115, (byte)27, (byte)-81, (byte)-110, (byte)-69, (byte)-35, (byte)-68, (byte)127, (byte)17, (byte)-39, (byte)92, (byte)65, (byte)31, (byte)16, (byte)90, (byte)-40, (byte)10, (byte)-63, (byte)49, (byte)-120, (byte)-91, (byte)-51, (byte)123, (byte)-67, (byte)45, (byte)116, (byte)-48, (byte)18, (byte)-72, (byte)-27, (byte)-76, (byte)-80, (byte)-119, (byte)105, (byte)-105, (byte)74, (byte)12, (byte)-106, (byte)119, (byte)126, (byte)101, (byte)-71, (byte)-15, (byte)9, (byte)-59, (byte)110, (byte)-58, (byte)-124, (byte)24, (byte)-16, (byte)125, (byte)-20, (byte)58, (byte)-36, (byte)77, (byte)32, (byte)121, (byte)-18, (byte)95, (byte)62, (byte)-41, (byte)-53, (byte)57, (byte)72};
    public static final int[] FK = new int[]{-1548633402, 1453994832, 1736282519, -1301273892};
    public static final int[] CK = new int[]{462357, 472066609, 943670861, 1415275113, 1886879365, -1936483679, -1464879427, -993275175, -521670923, -66909679, 404694573, 876298825, 1347903077, 1819507329, -2003855715, -1532251463, -1060647211, -589042959, -117504499, 337322537, 808926789, 1280531041, 1752135293, -2071227751, -1599623499, -1128019247, -656414995, -184876535, 269950501, 741554753, 1213159005, 1684763257};

    public SM4() {
    }

    private long GET_ULONG_BE(byte[] b, int i) {
        long n = (long)(b[i] & 255) << 24 | (long)((b[i + 1] & 255) << 16) | (long)((b[i + 2] & 255) << 8) | (long)(b[i + 3] & 255) & 4294967295L;
        return n;
    }

    private void PUT_ULONG_BE(long n, byte[] b, int i) {
        b[i] = (byte)((int)(255L & n >> 24));
        b[i + 1] = (byte)((int)(255L & n >> 16));
        b[i + 2] = (byte)((int)(255L & n >> 8));
        b[i + 3] = (byte)((int)(255L & n));
    }

    private long SHL(long x, int n) {
        return (x & -1L) << n;
    }

    private long ROTL(long x, int n) {
        return this.SHL(x, n) | x >> 32 - n;
    }

    private void SWAP(long[] sk, int i) {
        long t = sk[i];
        sk[i] = sk[31 - i];
        sk[31 - i] = t;
    }

    private byte sm4Sbox(byte inch) {
        int i = inch & 255;
        byte retVal = SboxTable[i];
        return retVal;
    }

    private long sm4Lt(long ka) {
        long bb = 0L;
        long c = 0L;
        byte[] a = new byte[4];
        byte[] b = new byte[4];
        this.PUT_ULONG_BE(ka, a, 0);
        b[0] = this.sm4Sbox(a[0]);
        b[1] = this.sm4Sbox(a[1]);
        b[2] = this.sm4Sbox(a[2]);
        b[3] = this.sm4Sbox(a[3]);
        bb = this.GET_ULONG_BE(b, 0);
        c = bb ^ this.ROTL(bb, 2) ^ this.ROTL(bb, 10) ^ this.ROTL(bb, 18) ^ this.ROTL(bb, 24);
        return c;
    }

    private long sm4F(long x0, long x1, long x2, long x3, long rk) {
        return x0 ^ this.sm4Lt(x1 ^ x2 ^ x3 ^ rk);
    }

    private long sm4CalciRK(long ka) {
        long bb = 0L;
        long rk = 0L;
        byte[] a = new byte[4];
        byte[] b = new byte[4];
        this.PUT_ULONG_BE(ka, a, 0);
        b[0] = this.sm4Sbox(a[0]);
        b[1] = this.sm4Sbox(a[1]);
        b[2] = this.sm4Sbox(a[2]);
        b[3] = this.sm4Sbox(a[3]);
        bb = this.GET_ULONG_BE(b, 0);
        rk = bb ^ this.ROTL(bb, 13) ^ this.ROTL(bb, 23);
        return rk;
    }

    private void sm4_setkey(long[] SK, byte[] key) {
        long[] MK = new long[4];
        long[] k = new long[36];
        int i = 0;
        MK[0] = this.GET_ULONG_BE(key, 0);
        MK[1] = this.GET_ULONG_BE(key, 4);
        MK[2] = this.GET_ULONG_BE(key, 8);
        MK[3] = this.GET_ULONG_BE(key, 12);
        k[0] = MK[0] ^ (long)FK[0];
        k[1] = MK[1] ^ (long)FK[1];
        k[2] = MK[2] ^ (long)FK[2];

        for(k[3] = MK[3] ^ (long)FK[3]; i < 32; ++i) {
            k[i + 4] = k[i] ^ this.sm4CalciRK(k[i + 1] ^ k[i + 2] ^ k[i + 3] ^ (long)CK[i]);
            SK[i] = k[i + 4];
        }

    }

    private void sm4_one_round(long[] sk, byte[] input, byte[] output) {
        int i = 0;
        long[] ulbuf = new long[36];
        ulbuf[0] = this.GET_ULONG_BE(input, 0);
        ulbuf[1] = this.GET_ULONG_BE(input, 4);
        ulbuf[2] = this.GET_ULONG_BE(input, 8);

        for(ulbuf[3] = this.GET_ULONG_BE(input, 12); i < 32; ++i) {
            ulbuf[i + 4] = this.sm4F(ulbuf[i], ulbuf[i + 1], ulbuf[i + 2], ulbuf[i + 3], sk[i]);
        }

        this.PUT_ULONG_BE(ulbuf[35], output, 0);
        this.PUT_ULONG_BE(ulbuf[34], output, 4);
        this.PUT_ULONG_BE(ulbuf[33], output, 8);
        this.PUT_ULONG_BE(ulbuf[32], output, 12);
    }

    private byte[] padding(byte[] input, int mode) {
        if(input == null) {
            return null;
        } else {
            byte[] ret = (byte[])null;
            if(mode == 1) {
                int p = 16 - input.length % 16;
                ret = new byte[input.length + p];
                System.arraycopy(input, 0, ret, 0, input.length);

                for(int i = 0; i < p; ++i) {
                    ret[input.length + i] = (byte)p;
                }
            } else {
                byte var6 = input[input.length - 1];
                ret = new byte[input.length - var6];
                System.arraycopy(input, 0, ret, 0, input.length - var6);
            }

            return ret;
        }
    }

    public void sm4_setkey_enc(SM4_Context ctx, byte[] key) throws Exception {
        if(ctx == null) {
            throw new Exception("ctx is null!");
        } else if(key != null && key.length == 16) {
            ctx.mode = 1;
            this.sm4_setkey(ctx.sk, key);
        } else {
            throw new Exception("key error!");
        }
    }

    public void sm4_setkey_dec(SM4_Context ctx, byte[] key) throws Exception {
        if(ctx == null) {
            throw new Exception("ctx is null!");
        } else if(key != null && key.length == 16) {
            boolean i = false;
            ctx.mode = 0;
            this.sm4_setkey(ctx.sk, key);

            for(int var4 = 0; var4 < 16; ++var4) {
                this.SWAP(ctx.sk, var4);
            }

        } else {
            throw new Exception("key error!");
        }
    }

    public byte[] sm4_crypt_ecb(SM4_Context ctx, byte[] input) throws Exception {
        if(input == null) {
            throw new Exception("input is null!");
        } else {
            if(ctx.isPadding && ctx.mode == 1) {
                input = this.padding(input, 1);
            }

            int length = input.length;
            ByteArrayInputStream bins = new ByteArrayInputStream(input);

            ByteArrayOutputStream bous;
            byte[] output;
            for(bous = new ByteArrayOutputStream(); length > 0; length -= 16) {
                output = new byte[16];
                byte[] out = new byte[16];
                bins.read(output);
                this.sm4_one_round(ctx.sk, output, out);
                bous.write(out);
            }

            output = bous.toByteArray();
            if(ctx.isPadding && ctx.mode == 0) {
                output = this.padding(output, 0);
            }

            bins.close();
            bous.close();
            return output;
        }
    }

    public byte[] sm4_crypt_cbc(SM4_Context ctx, byte[] iv, byte[] input) throws Exception {
        if(iv != null && iv.length == 16) {
            if(input == null) {
                throw new Exception("input is null!");
            } else {
                if(ctx.isPadding && ctx.mode == 1) {
                    input = this.padding(input, 1);
                }

                boolean i = false;
                int length = input.length;
                ByteArrayInputStream bins = new ByteArrayInputStream(input);
                ByteArrayOutputStream bous = new ByteArrayOutputStream();
                byte[] output;
                byte[] in;
                byte[] out;
                int var12;
                if(ctx.mode != 1) {
                    for(output = new byte[16]; length > 0; length -= 16) {
                        in = new byte[16];
                        out = new byte[16];
                        byte[] out1 = new byte[16];
                        bins.read(in);
                        System.arraycopy(in, 0, output, 0, 16);
                        this.sm4_one_round(ctx.sk, in, out);

                        for(var12 = 0; var12 < 16; ++var12) {
                            out1[var12] = (byte)(out[var12] ^ iv[var12]);
                        }

                        System.arraycopy(output, 0, iv, 0, 16);
                        bous.write(out1);
                    }
                } else {
                    while(length > 0) {
                        output = new byte[16];
                        in = new byte[16];
                        out = new byte[16];
                        bins.read(output);

                        for(var12 = 0; var12 < 16; ++var12) {
                            in[var12] = (byte)(output[var12] ^ iv[var12]);
                        }

                        this.sm4_one_round(ctx.sk, in, out);
                        System.arraycopy(out, 0, iv, 0, 16);
                        bous.write(out);
                        length -= 16;
                    }
                }

                output = bous.toByteArray();
                if(ctx.isPadding && ctx.mode == 0) {
                    output = this.padding(output, 0);
                }

                bins.close();
                bous.close();
                return output;
            }
        } else {
            throw new Exception("iv error!");
        }
    }
}
