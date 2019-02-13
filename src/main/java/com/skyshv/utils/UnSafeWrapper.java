package com.skyshv.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnSafeWrapper {
    public static Unsafe unSafe;

    static {
        final Field theUnsafe;
        try {
            theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unSafe = (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
