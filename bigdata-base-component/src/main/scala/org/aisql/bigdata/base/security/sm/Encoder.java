package org.aisql.bigdata.base.security.sm;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

import java.io.IOException;
import java.io.OutputStream;

public interface Encoder {
    int encode(byte[] var1, int var2, int var3, OutputStream var4) throws IOException;

    int decode(byte[] var1, int var2, int var3, OutputStream var4) throws IOException;

    int decode(String var1, OutputStream var2) throws IOException;
}
