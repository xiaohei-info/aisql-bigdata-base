package org.aisql.bigdata.base.security.sm;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */
import java.util.Iterator;

public interface Iterable<T> extends java.lang.Iterable<T> {
    @Override
    Iterator<T> iterator();
}
