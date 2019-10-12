package com.xinyan.bigdata.base.security.common;

/**
 * Author: xiaohei
 * Date: 2019/9/20
 * Email: xiaohei.info@gmail.com
 * Host: xiaohei.info
 */

import com.sun.istack.internal.Nullable;

public final class Strings {
    private Strings() {
    }

    public static String nullToEmpty(@Nullable String string) {
        return string == null ? "" : string;
    }

    public static String padStart(String string, int minLength, char padChar) {
        checkNotNull(string);
        if (string.length() >= minLength) {
            return string;
        } else {
            StringBuilder sb = new StringBuilder(minLength);

            for (int i = string.length(); i < minLength; ++i) {
                sb.append(padChar);
            }

            sb.append(string);
            return sb.toString();
        }
    }

    public static String padEnd(String string, int minLength, char padChar) {
        checkNotNull(string);
        if (string.length() >= minLength) {
            return string;
        } else {
            StringBuilder sb = new StringBuilder(minLength);
            sb.append(string);

            for (int i = string.length(); i < minLength; ++i) {
                sb.append(padChar);
            }

            return sb.toString();
        }
    }

    public static String commonPrefix(CharSequence a, CharSequence b) {
        checkNotNull(a);
        checkNotNull(b);
        int maxPrefixLength = Math.min(a.length(), b.length());

        int p;
        for (p = 0; p < maxPrefixLength && a.charAt(p) == b.charAt(p); ++p) {
            ;
        }

        if (validSurrogatePairAt(a, p - 1) || validSurrogatePairAt(b, p - 1)) {
            --p;
        }

        return a.subSequence(0, p).toString();
    }

    public static String commonSuffix(CharSequence a, CharSequence b) {
        checkNotNull(a);
        checkNotNull(b);
        int maxSuffixLength = Math.min(a.length(), b.length());

        int s;
        for (s = 0; s < maxSuffixLength && a.charAt(a.length() - s - 1) == b.charAt(b.length() - s - 1); ++s) {
            ;
        }

        if (validSurrogatePairAt(a, a.length() - s - 1) || validSurrogatePairAt(b, b.length() - s - 1)) {
            --s;
        }

        return a.subSequence(a.length() - s, a.length()).toString();
    }

    static boolean validSurrogatePairAt(CharSequence string, int index) {
        return index >= 0 && index <= string.length() - 2 && Character.isHighSurrogate(string.charAt(index)) && Character.isLowSurrogate(string.charAt(index + 1));
    }

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }
}
