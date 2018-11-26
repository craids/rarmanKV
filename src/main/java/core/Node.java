package core;

import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;

final class Node {
    public static final int MIN_FANOUT = 16;
    public static final int MAX_FANOUT = 2 * MIN_FANOUT - 1;

    // Linear search seems about ~20% faster than binary (for MIN_FANOUT = 8 at least)
    public static final boolean BINARY_SEARCH = false;

    private static final Unsafe UNSAFE;
    private static final long OFFSET0;
    private static final int POINTER_SIZE;

    static {
        try {
            Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            UNSAFE = unsafeConstructor.newInstance();
        } catch (NoSuchElementException | IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        final TreeMap<Long, Field> fieldByOffset = new TreeMap<>();
        for (Field f : Node.class.getDeclaredFields()) {
            if (Object.class.isAssignableFrom(f.getType()) && (f.getModifiers() & Modifier.STATIC) == 0) {
                final long offset = UNSAFE.objectFieldOffset(f);
                if (fieldByOffset.put(offset, f) != null) {
                    throw new IllegalStateException("Multiple fields seem to share a single offset " + offset);
                }
            }
        }

        if (fieldByOffset.size() != MAX_FANOUT*2) {
            throw new IllegalStateException("Expected " + (MAX_FANOUT * 2) + " object fields, got " + fieldByOffset.size());
        }

        // This might differ from ADDRESS_SIZE if compressed OOPs are in use
        POINTER_SIZE = UNSAFE.arrayIndexScale(Object[].class);

        final Iterator<Map.Entry<Long, Field>> it = fieldByOffset.entrySet().iterator();
        long lastOffset = OFFSET0 = it.next().getKey();
        while (it.hasNext()) {
            final Map.Entry<Long, Field> e = it.next();
            final long offset = e.getKey();
            if (offset != lastOffset + POINTER_SIZE) {
                throw new IllegalStateException("Expected object fields to be contiguous in memory but " + e.getValue() + " is at " + offset + " and the last one was at " + lastOffset);
            }

            lastOffset = offset;
        }
    }

    public int size;
    private Object
            o00, o01, o02, o03, o04, o05, o06, o07,
            o08, o09, o10, o11, o12, o13, o14, o15,
            o16, o17, o18, o19, o20, o21, o22, o23,
            o24, o25, o26, o27, o28, o29;
    private Object
            o30, o31, o32, o33, o34, o35, o36, o37,
            o38, o39, o40, o41, o42, o43, o44, o45,
            o46, o47, o48, o49, o50, o51, o52, o53,
            o54, o55, o56, o57, o58, o59, o60, o61;
    /*private Object
            o62, o63, o64, o65, o66, o67, o68, o69,
            o70, o71, o72, o73, o74, o75, o76, o77,
            o78, o79, o80, o81, o82, o83, o84, o85;*/

    public Object get(int i) {
        return UNSAFE.getObject(this, OFFSET0 + i * POINTER_SIZE);
    }

    public void set(int i, Object x) {
        UNSAFE.putObject(this, OFFSET0 + i * POINTER_SIZE, x);
    }

    public int binarySearch(int fromIndex, int toIndex, Object key, Comparator c) {
        if (c == null) {
            return binarySearch(fromIndex, toIndex, key);
        }

        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Object midVal = get(mid);
            int cmp = c.compare(midVal, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    public int binarySearch(int fromIndex, int toIndex, Object key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            @SuppressWarnings("rawtypes")
            Comparable midVal = (Comparable)this.get(mid);
            @SuppressWarnings("unchecked")
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    public static void arraycopy(Node src, int srcIndex, Node dst, int dstIndex, int size) {
        if (size < 0 || srcIndex + size > MAX_FANOUT*2 || dstIndex + size > MAX_FANOUT*2) {
            throw new ArrayIndexOutOfBoundsException();
        }

        if (dst == src && srcIndex < dstIndex) {
            for (int i = size - 1; i >= 0; i--) {
                dst.set(dstIndex + i, src.get(srcIndex + i));
            }
        } else {
            for (int i = 0; i < size; i++) {
                dst.set(dstIndex + i, src.get(srcIndex + i));
            }
        }
    }
}