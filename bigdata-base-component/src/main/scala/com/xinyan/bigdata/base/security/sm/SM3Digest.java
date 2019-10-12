package com.xinyan.bigdata.base.security.sm;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */
public class SM3Digest {
    private static final int BYTE_LENGTH = 32;
    private static final int BLOCK_LENGTH = 64;
    private static final int BUFFER_LENGTH = 64;
    private byte[] xBuf = new byte[64];
    private int xBufOff;
    private byte[] V;
    private int cntBlock;

    public SM3Digest() {
        this.V = (byte[])SM3.iv.clone();
        this.cntBlock = 0;
    }

    public SM3Digest(SM3Digest t) {
        this.V = (byte[])SM3.iv.clone();
        this.cntBlock = 0;
        System.arraycopy(t.xBuf, 0, this.xBuf, 0, t.xBuf.length);
        this.xBufOff = t.xBufOff;
        System.arraycopy(t.V, 0, this.V, 0, t.V.length);
    }

    public int doFinal(byte[] out, int outOff) {
        byte[] tmp = this.doFinal();
        System.arraycopy(tmp, 0, out, 0, tmp.length);
        return 32;
    }

    public void reset() {
        this.xBufOff = 0;
        this.cntBlock = 0;
        this.V = (byte[])SM3.iv.clone();
    }

    public void update(byte[] in, int inOff, int len) {
        int partLen = 64 - this.xBufOff;
        int inputLen = len;
        int dPos = inOff;
        if(partLen < len) {
            System.arraycopy(in, inOff, this.xBuf, this.xBufOff, partLen);
            inputLen = len - partLen;
            dPos = inOff + partLen;
            this.doUpdate();

            while(inputLen > 64) {
                System.arraycopy(in, dPos, this.xBuf, 0, 64);
                inputLen -= 64;
                dPos += 64;
                this.doUpdate();
            }
        }

        System.arraycopy(in, dPos, this.xBuf, this.xBufOff, inputLen);
        this.xBufOff += inputLen;
    }

    private void doUpdate() {
        byte[] B = new byte[64];

        for(int i = 0; i < 64; i += 64) {
            System.arraycopy(this.xBuf, i, B, 0, B.length);
            this.doHash(B);
        }

        this.xBufOff = 0;
    }

    private void doHash(byte[] B) {
        byte[] tmp = SM3.CF(this.V, B);
        System.arraycopy(tmp, 0, this.V, 0, this.V.length);
        ++this.cntBlock;
    }

    private byte[] doFinal() {
        byte[] B = new byte[64];
        byte[] buffer = new byte[this.xBufOff];
        System.arraycopy(this.xBuf, 0, buffer, 0, buffer.length);
        byte[] tmp = SM3.padding(buffer, this.cntBlock);

        for(int i = 0; i < tmp.length; i += 64) {
            System.arraycopy(tmp, i, B, 0, B.length);
            this.doHash(B);
        }

        return this.V;
    }

    public void update(byte in) {
        byte[] buffer = new byte[]{in};
        this.update(buffer, 0, 1);
    }

    public int getDigestSize() {
        return 32;
    }

    public static void main(String[] args) {
        byte[] md = new byte[32];
        byte[] msg1 = "ererfeiisgod".getBytes();
        SM3Digest sm3 = new SM3Digest();
        sm3.update(msg1, 0, msg1.length);
        sm3.doFinal(md, 0);
        String s = new String(Hex.encode(md));
        System.out.println(s.toUpperCase());
    }
}
