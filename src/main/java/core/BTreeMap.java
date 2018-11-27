package core;

import java.util.*;

import static core.Node.BINARY_SEARCH;
import static core.Node.MAX_FANOUT;
import static core.Node.MIN_FANOUT;

/**
 * A B-tree based {@link NavigableMap} implementation.
 * <p>
 * All values in the map are stored at leaf nodes, and all leaf nodes are at the same depth. This ensures that accesses
 * to the map via e.g. {@code get}, {@code put} and {@code remove} all take log(n) time. In contrast to a balanced binary
 * tree (like that used by {@link java.util.TreeMap}), each node may holds more than one key. This makes the datastructure
 * more cache-friendly: you can expect it to be around 50% faster than {@code TreeMap} for workloads that do not fit in cache.
 * <p>
 * The map is sorted either according to the Comparable method of the key type, or via a user-supplied {@code Comparator}.
 * This ordering should be consistent with {@code equals}.
 * <p>
 * The implementation is unsynchronized, and there are no guarantees as to what will happen if you make use of iterator
 * that was created before some operation that modified the map.
 * <p>
 * {@code Entry} instances returned by this class are immutable and hence do not support the {@link Entry#setValue(Object)} method.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public class BTreeMap<K, V> implements NavigableMap<K, V> {
    public static <K extends Comparable<? super K>, V> BTreeMap<K, V> create() {
        return new BTreeMap<K, V>(null);
    }

    /** @param comparator If null, the natural order of the keys will be used */
    public static <K, V> BTreeMap<K, V> create(Comparator<K> comparator) {
        return new BTreeMap<K, V>(comparator);
    }

    public static <K, V> BTreeMap<K, V> create(SortedMap<K, ? extends V> that) {
        final BTreeMap<K, V> result = new BTreeMap<>(that.comparator());
        result.putAll(that);
        return result;
    }

    public static <K, V> BTreeMap<K, V> create(Map<? extends K, ? extends V> that) {
        final BTreeMap<K, V> result = new BTreeMap<>(null);
        result.putAll(that);
        return result;
    }

    // Internal nodes and leaf nodes are both represented by an instance of the Node class. A Node is essentially
    // an Object[MAX_FANOUT * 2] with a size. For an internal node:
    //   - The first MAX_FANOUT - 1 elements of this will refer to keys
    //   - The next MAX_FANOUT elements will hold references to the Object[] of child nodes
    //   - The final element will hold a reference to a int[] of child node sizes
    //
    // For a leaf node:
    //   - The first MAX_FANOUT elements will refer to keys
    //   - The next MAX_FANOUT elements will refer to values
    //
    // Instead of Node, I used to just use a Object[] instead, with the size of all sibling nodes stored contiguously in another int[].
    // This was sort of fine but incurred more indirections & was fiddly to program against. Replacing it with this scheme
    // (such that the size and Objects are stored contiguously) sped up my get benchmark from 4.8M ops/sec to 5.3M ops/sec.
    //
    // FIXME: the single element at the end of each internal node is unused. What to do about this? Use for parent link -- might help us avoid allocation in iterator?
    private static class BubbledInsertion {
        private final Node leftObjects, rightObjects;
        private final Object separator; // The seperator key is <= all keys in the right and > all keys in the left

        private BubbledInsertion(Node leftObjects, Node rightObjects, Object separator) {
            this.leftObjects = leftObjects;
            this.rightObjects = rightObjects;
            this.separator = separator;
        }
    }

    private static class Leaf {
        private Leaf() {}

        public static int find(Node keysValues, Object key, Comparator comparator) {
            final int size = keysValues.size;
            if (BINARY_SEARCH) {
                return keysValues.binarySearch(0, size, key, comparator);
            } else {
                for (int i = 0; i < size; i++) {
                    final Object checkKey = keysValues.get(i);
                    final int cmp = comparator == null ? ((Comparable)checkKey).compareTo(key) : comparator.compare(checkKey, key);
                    if (cmp == 0) {
                        return i;
                    } else if (cmp > 0) {
                        return -i - 1;
                    }
                }

                return -size - 1;
            }
        }

        public static int getKeyIndex(int index) {
            return index;
        }

        public static Object getKey(Node keysValues, int index) {
            return keysValues.get(index);
        }

        public static int getValueIndex(int index) {
            return MAX_FANOUT + index;
        }

        public static Object getValue(Node keysValues, int index) {
            return keysValues.get(getValueIndex(index));
        }

        public static Object putOrDieIfFull(Node keysValues, Object key, Object value, Comparator comparator) {
            int index = find(keysValues, key, comparator);
            return putAtIndex(keysValues, index, key, value);
        }

        public static boolean canPutAtIndex(int size, int index) {
            return index >= 0 || size < MAX_FANOUT;
        }

        /** @param index must be the index of key in the leaf, using same convention as Arrays.binarySearch */
        public static Object putAtIndex(Node keysValues, int index, Object key, Object value) {
            assert canPutAtIndex(keysValues.size, index);

            final Object result;
            if (index < 0) {
                final int size = keysValues.size;
                assert size < MAX_FANOUT;

                final int insertionPoint = -(index + 1);
                Node.arraycopy(keysValues,              insertionPoint, keysValues,              insertionPoint + 1, size - insertionPoint);
                Node.arraycopy(keysValues, MAX_FANOUT + insertionPoint, keysValues, MAX_FANOUT + insertionPoint + 1, size - insertionPoint);
                keysValues.size = size + 1;

                keysValues.set(insertionPoint, key);

                result = null;
                index = insertionPoint;
            } else {
                result = keysValues.get(MAX_FANOUT + index);
            }

            keysValues.set(MAX_FANOUT + index, value);
            return result;
        }

        private static void copy(Node srcKeysValues, int srcIndex, Node dstKeysValues, int dstIndex, int size) {
            Node.arraycopy(srcKeysValues, srcIndex,              dstKeysValues, dstIndex,              size);
            Node.arraycopy(srcKeysValues, srcIndex + MAX_FANOUT, dstKeysValues, dstIndex + MAX_FANOUT, size);
        }

        // This splits the leaf (of size MAX_FANOUT == 2 * MIN_FANOUT - 1) plus one extra item into two new
        // leaves, each of size MIN_FANOUT.
        public static BubbledInsertion bubblePutAtIndex(Node keysValues, int index, Object key, Object value) {
            assert !canPutAtIndex(keysValues.size, index);
            assert keysValues.size == MAX_FANOUT; // i.e. implies index < 0

            int insertionPoint = -(index + 1);
            final Node l = new Node(), r = new Node();
            l.size = r.size = MIN_FANOUT;

            if (insertionPoint < MIN_FANOUT) {
                copy(keysValues, 0,                           l, 0,                  insertionPoint);
                copy(keysValues, insertionPoint,              l, insertionPoint + 1, MIN_FANOUT - insertionPoint - 1);
                copy(keysValues, MIN_FANOUT - 1,              r, 0,                  MIN_FANOUT);

                l.set(insertionPoint,              key);
                l.set(insertionPoint + MAX_FANOUT, value);
            } else {
                insertionPoint -= MIN_FANOUT;

                copy(keysValues, 0,                           l, 0,                  MIN_FANOUT);
                copy(keysValues, MIN_FANOUT,                  r, 0,                  insertionPoint);
                copy(keysValues, MIN_FANOUT + insertionPoint, r, insertionPoint + 1, MIN_FANOUT - insertionPoint - 1);

                r.set(insertionPoint,              key);
                r.set(insertionPoint + MAX_FANOUT, value);
            }

            return new BubbledInsertion(l, r, r.get(0));
        }
    }

    private static class Internal {
        private Internal() {}

        private static int getKeyIndex(int index) {
            return index;
        }

        private static Object getKey(Node repr, int index) {
            return repr.get(getKeyIndex(index));
        }

        private static int getNodeIndex(int index) {
            return MAX_FANOUT - 1 + index;
        }

        private static Node getNode(Node repr, int index) {
            return (Node)repr.get(getNodeIndex(index));
        }

        /** Always returns a valid index into the nodes array. Keys in the node indicated in the index will be >= key */
        public static int find(Node repr, Object key, Comparator comparator) {
            final int size = repr.size;
            if (BINARY_SEARCH) {
                final int index = repr.binarySearch(0, size - 1, key, comparator);
                return index < 0 ? -(index + 1) : index + 1;
            } else {
                // Tried doing the comparator == null check outside the loop, but not a significant speed boost
                // Tried not exiting early (improves branch prediction) but significantly worse.
                int i;
                for (i = 0; i < size - 1; i++) {
                    final Object checkKey = repr.get(i);
                    final int cmp = comparator == null ? ((Comparable)checkKey).compareTo(key) : comparator.compare(checkKey, key);
                    if (cmp > 0) {
                        return i;
                    }
                }

                return i;
            }
        }

        public static boolean canPutAtIndex(int size) {
            return size < MAX_FANOUT;
        }

        public static void putAtIndex(Node repr, int index, BubbledInsertion toBubble) {
            assert canPutAtIndex(repr.size);

            // Tree:         Bubbled input:
            //  Key   0 1     Key    S
            //  Node 0 1 2    Node 1A 1B
            // If inserting at nodeIndex 1, shuffle things to:
            //  Key   0  S  1
            //  Node 0 1A 1B 2

            final int size = repr.size++;
            Node.arraycopy(repr,  getKeyIndex (index),     repr,  getKeyIndex (index + 1), size - index - 1);
            Node.arraycopy(repr,  getNodeIndex(index + 1), repr,  getNodeIndex(index + 2), size - index - 1);

            repr.set(getKeyIndex (index)    , toBubble.separator);
            repr.set(getNodeIndex(index)    , toBubble.leftObjects);
            repr.set(getNodeIndex(index + 1), toBubble.rightObjects);
        }

        private static void deleteAtIndex(Node node, int index) {
            final int size = --node.size;
            Node.arraycopy(node, getKeyIndex (index),     node, getKeyIndex (index - 1), size - index);
            Node.arraycopy(node, getNodeIndex(index + 1), node, getNodeIndex(index),     size - index);

            // Avoid memory leaks
            node.set(getKeyIndex(size - 1), null);
            node.set(getNodeIndex(size),    null);
        }

        public static BubbledInsertion bubblePutAtIndex(Node repr, int nodeIndex, BubbledInsertion toBubble) {
            assert !canPutAtIndex(repr.size); // i.e. size == MAX_FANOUT

            // Tree:         Bubbled input:
            //  Key   0 1     Key    S
            //  Node 0 1 2    Node 1A  1B
            //
            // If inserting at nodeIndex 1, split things as:
            //
            // Separator: S
            // Left bubbled:  Right bubbled:
            //  Key   0        Key    1
            //  Node 0 1A      Node 1B 2

            final Node l = new Node(), r = new Node();
            l.size = r.size = MIN_FANOUT;

            final Object separator;
            if (nodeIndex == MIN_FANOUT - 1) {
                separator = toBubble.separator;

                Node.arraycopy(repr, getKeyIndex(0),              l, getKeyIndex(0), MIN_FANOUT - 1);
                Node.arraycopy(repr, getKeyIndex(MIN_FANOUT - 1), r, getKeyIndex(0), MIN_FANOUT - 1);

                Node.arraycopy(repr,  getNodeIndex(0),          l,      getNodeIndex(0), MIN_FANOUT - 1);
                Node.arraycopy(repr,  getNodeIndex(MIN_FANOUT), r,      getNodeIndex(1), MIN_FANOUT - 1);

                l.set(getNodeIndex(MIN_FANOUT - 1), toBubble.leftObjects);
                r.set(getNodeIndex(0),              toBubble.rightObjects);
            } else if (nodeIndex < MIN_FANOUT) {
                separator = getKey(repr, MIN_FANOUT - 2);

                Node.arraycopy(repr, getKeyIndex(0),              l, getKeyIndex(0),             nodeIndex);
                Node.arraycopy(repr, getKeyIndex(nodeIndex),      l, getKeyIndex(nodeIndex + 1), MIN_FANOUT - nodeIndex - 2);
                Node.arraycopy(repr, getKeyIndex(MIN_FANOUT - 1), r, getKeyIndex(0),             MIN_FANOUT - 1);

                Node.arraycopy(repr,  getNodeIndex(0),              l,      getNodeIndex(0),             nodeIndex);
                Node.arraycopy(repr,  getNodeIndex(nodeIndex + 1),  l,      getNodeIndex(nodeIndex + 2), MIN_FANOUT - nodeIndex - 2);
                Node.arraycopy(repr,  getNodeIndex(MIN_FANOUT - 1), r,      getNodeIndex(0),             MIN_FANOUT);

                l.set(getKeyIndex(nodeIndex), toBubble.separator);
                l.set(getNodeIndex(nodeIndex),     toBubble.leftObjects);
                l.set(getNodeIndex(nodeIndex + 1), toBubble.rightObjects);
            } else {
                nodeIndex -= MIN_FANOUT;
                // i.e. 0 <= nodeIndex < MIN_FANOUT - 1

                separator = getKey(repr, MIN_FANOUT - 1);

                Node.arraycopy(repr, getKeyIndex(0),                      l, getKeyIndex(0),             MIN_FANOUT - 1);
                Node.arraycopy(repr, getKeyIndex(MIN_FANOUT),             r, getKeyIndex(0),             nodeIndex);
                Node.arraycopy(repr, getKeyIndex(MIN_FANOUT + nodeIndex), r, getKeyIndex(nodeIndex + 1), MIN_FANOUT - nodeIndex - 2);

                Node.arraycopy(repr,  getNodeIndex(0),                          l,      getNodeIndex(0),             MIN_FANOUT);
                Node.arraycopy(repr,  getNodeIndex(MIN_FANOUT),                 r,      getNodeIndex(0),             nodeIndex);
                Node.arraycopy(repr,  getNodeIndex(MIN_FANOUT + nodeIndex + 1), r,      getNodeIndex(nodeIndex + 2), MIN_FANOUT - nodeIndex - 2);

                r.set(getKeyIndex(nodeIndex), toBubble.separator);
                r.set(getNodeIndex(nodeIndex),     toBubble.leftObjects);
                r.set(getNodeIndex(nodeIndex + 1), toBubble.rightObjects);
            }

            return new BubbledInsertion(l, r, separator);
        }
    }

    private final Comparator<? super K> comparator;

    // Allocate these lazily to optimize allocation of lots of empty BTreeMaps
    private Node rootObjects;

    private int depth; // Number of levels of internal nodes in the tree
    private int size;

    private BTreeMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
    }

    void checkAssumingKeysNonNull() {
        if (rootObjects != null) {
            checkCore(rootObjects, depth, null, null, Bound.MISSING, Bound.MISSING);
        }
    }

    private void checkInRange(Object k, Object min, Object max, Bound minBound, Bound maxBound) {
        assert minBound.lt(min, k, comparator) && maxBound.lt(k, max, comparator);
    }

    private void checkCore(Node repr, int depth, Object min, Object max, Bound minBound, Bound maxBound) {
        assert repr.size <= Node.MAX_FANOUT;
        if (depth == this.depth) {
            // The root node may be smaller than others
            if (depth > 0) {
                assert repr.size >= 2;
            }
        } else {
            assert repr.size >= Node.MIN_FANOUT;
        }

        final int size = repr.size;
        if (depth == 0) {
            int i;
            for (i = 0; i < size; i++) {
                Object k = Leaf.getKey(repr, i);
                checkInRange(k, min, max, minBound, maxBound);
                assert k != null;
            }

            // To avoid memory leaks
            for (; i < Node.MAX_FANOUT; i++) {
                assert Leaf.getKey(repr, i) == null;
            }
        } else {
            {
                int i;
                for (i = 0; i < size - 1; i++) {
                    Object k = Internal.getKey(repr, i);
                    checkInRange(k, min, max, minBound, maxBound);
                    assert k != null;
                }

                // To avoid memory leaks
                for (; i < Node.MAX_FANOUT - 1; i++) {
                    assert Internal.getKey(repr, i) == null;
                }
            }

            {
                int i;
                checkCore    (Internal.getNode(repr, 0),        depth - 1, min,                             Internal.getKey(repr, 0), minBound,        Bound.EXCLUSIVE);
                for (i = 1; i < size - 1; i++) {
                    checkCore(Internal.getNode(repr, i),        depth - 1, Internal.getKey(repr, i - 1),    Internal.getKey(repr, i), Bound.INCLUSIVE, Bound.EXCLUSIVE);
                }
                checkCore    (Internal.getNode(repr, size - 1), depth - 1, Internal.getKey(repr, size - 2), max,                      Bound.INCLUSIVE, maxBound);

                // To avoid memory leaks
                for (i = size; i < Node.MAX_FANOUT; i++) {
                    assert Internal.getNode(repr, i) == null;
                }
            }
        }
    }

    @Override
    public void clear() {
        rootObjects = null;
        depth = 0;
        size = 0;
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> that) {
        if (that instanceof SortedMap && Objects.equals(this.comparator(), ((SortedMap)that).comparator())) {
            // TODO: fastpath?
        }

        for (Map.Entry<? extends K, ? extends V> e : that.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public V get(Object key) {
        return getOrDefault(key, null);
    }

    @Override
    public V getOrDefault(Object key, V dflt) {
        final Comparator<? super K> comparator = this.comparator;

        Node nextObjects = rootObjects;
        int depth = this.depth;

        if (nextObjects == null) {
            return dflt;
        }

        while (depth-- > 0) {
            final int ix = Internal.find(nextObjects, key, comparator);
            nextObjects = Internal.getNode(nextObjects, ix);
        }

        final int ix = Leaf.find(nextObjects, key, comparator);
        if (ix < 0) {
            return dflt;
        } else {
            return (V)Leaf.getValue(nextObjects, ix);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        if (rootObjects == null) {
            return false;
        }

        Node nextObjects = rootObjects;
        int depth = this.depth;
        while (depth-- > 0) {
            final int ix = Internal.find(nextObjects, key, comparator);
            nextObjects = Internal.getNode(nextObjects, ix);
        }

        final int ix = Leaf.find(nextObjects, key, comparator);
        return ix >= 0;
    }

    @Override
    public V put(K key, V value) {
        if (rootObjects == null) {
            rootObjects = new Node();
            final Object result = Leaf.putOrDieIfFull(rootObjects, key, value, comparator);
            assert result == null;

            this.size = 1;
            return null;
        }

        final Object[] resultBox = new Object[1];
        final BubbledInsertion toBubble = putInternal(key, value, rootObjects, this.depth, resultBox);
        if (toBubble == null) {
            return (V)resultBox[0];
        }

        this.rootObjects = new Node();
        this.rootObjects.size = 2;
        this.rootObjects.set(Internal.getKeyIndex (0), toBubble.separator);
        this.rootObjects.set(Internal.getNodeIndex(0), toBubble.leftObjects);
        this.rootObjects.set(Internal.getNodeIndex(1), toBubble.rightObjects);

        this.depth++;
        return null;
    }

    private BubbledInsertion putInternal(K key, V value, Node nextObjects, int depth, Object[] resultBox) {
        final int size = nextObjects.size;
        if (depth == 0) {
            final int nodeIndex = Leaf.find(nextObjects, key, comparator);
            if (nodeIndex < 0) this.size++;

            if (Leaf.canPutAtIndex(size, nodeIndex)) {
                resultBox[0] = Leaf.putAtIndex(nextObjects, nodeIndex, key, value);
                return null;
            }

            return Leaf.bubblePutAtIndex(nextObjects, nodeIndex, key, value);
        } else {
            final int nodeIndex = Internal.find(nextObjects, key, comparator);

            final BubbledInsertion toBubble = putInternal(key, value, Internal.getNode(nextObjects, nodeIndex), depth - 1, resultBox);
            if (toBubble == null) {
                return null;
            }

            if (Internal.canPutAtIndex(size)) {
                Internal.putAtIndex(nextObjects, nodeIndex, toBubble);
                return null;
            }

            return Internal.bubblePutAtIndex(nextObjects, nodeIndex, toBubble);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    public boolean containsValue(Object value) {
        return values().stream().anyMatch(v -> Objects.equals(v, value));
    }

    @Override
    public String toString() {
        if (false) {
            return rootObjects == null ? "{}" : toStringInternal(rootObjects, depth);
        } else {
            return Iterables.toMapString(this.entrySet());
        }
    }

    @Override
    public boolean equals(Object that) {
        return SortedMaps.equals(this, that);
    }

    @Override
    public int hashCode() {
        return Iterables.hashCode(entrySet());
    }

    private static String toStringInternal(Node repr, int depth) {
        if (depth == 0) {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < repr.size; i++) {
                if (sb.length() != 0) sb.append(", ");
                sb.append(Leaf.getKey(repr, i)).append(": ").append(Leaf.getValue(repr, i));
            }

            return sb.toString();
        } else {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < repr.size; i++) {
                if (sb.length() != 0) {
                    sb.append(" |").append(Internal.getKey(repr, i - 1)).append("| ");
                }
                final Node node = Internal.getNode(repr, i);
                sb.append("{").append(toStringInternal(node, depth - 1)).append("}");
            }

            return sb.toString();
        }
    }

    static <K, V> K getEntryKey(Entry<K, V> e) {
        return e == null ? null : e.getKey();
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        if (rootObjects == null) {
            return null;
        }

        final int depth = this.depth;

        Node backtrackParent = null; // Deepest internal node on the path to "key" which has a child prior to the one we descended into
        int backtrackIndex = -1;     // Index of that prior child
        int backtrackDepth = -1;     // Depth of that internal node

        Node repr = rootObjects;
        for (int i = 0; i < depth; i++) {
            final int index = Internal.find(repr, key, comparator);
            if (index > 0) {
                backtrackParent = repr;
                backtrackIndex = index - 1;
                backtrackDepth = i;
            }
            repr = Internal.getNode(repr, index);
        }

        final int leafIndex = Leaf.find(repr, key, comparator);
        final int insertionPoint = leafIndex >= 0 ? leafIndex : -(leafIndex + 1);
        final int returnIndex;
        if (insertionPoint > 0) {
            returnIndex = insertionPoint - 1;
        } else {
            // insertionPoint == 0: we need to find the last item in the prior leaf node.
            if (backtrackParent == null) {
                // Oh -- that was the first leaf node
                return null;
            }

            repr = backtrackParent;
            int index = backtrackIndex;
            for (int i = backtrackDepth; i < depth; i++) {
                repr = Internal.getNode(repr, index);
                index = repr.size - 1;
            }

            returnIndex = index;
        }

        return new AbstractMap.SimpleImmutableEntry<>(
            (K)Leaf.getKey  (repr, returnIndex),
            (V)Leaf.getValue(repr, returnIndex)
        );
    }

    @Override
    public K lowerKey(K key) {
        return getEntryKey(lowerEntry(key));
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        if (rootObjects == null) {
            return null;
        }

        final int depth = this.depth;

        Node backtrackParent = null; // Deepest internal node on the path to "key" which has a child prior to the one we descended into
        int backtrackIndex = -1;     // Index of that prior child
        int backtrackDepth = -1;     // Depth of that internal node

        Node repr = rootObjects;
        for (int i = 0; i < depth; i++) {
            final int index = Internal.find(repr, key, comparator);
            if (index > 0) {
                backtrackParent = repr;
                backtrackIndex = index - 1;
                backtrackDepth = i;
            }
            repr = Internal.getNode(repr, index);
        }

        final int leafIndex = Leaf.find(repr, key, comparator);
        final int returnIndex;
        if (leafIndex >= 0) {
            returnIndex = leafIndex;
        } else {
            final int insertionPoint = -(leafIndex + 1);
            if (insertionPoint > 0) {
                returnIndex = insertionPoint - 1;
            } else {
                // insertionPoint == 0: we need to find the last item in the prior leaf node.
                if (backtrackParent == null) {
                    // Oh -- that was the first leaf node
                    return null;
                }

                repr = backtrackParent;
                int index = backtrackIndex;
                for (int i = backtrackDepth; i < depth; i++) {
                    repr = Internal.getNode(repr, index);
                    index = repr.size - 1;
                }

                returnIndex = index;
            }
        }

        return new AbstractMap.SimpleImmutableEntry<>(
            (K)Leaf.getKey  (repr, returnIndex),
            (V)Leaf.getValue(repr, returnIndex)
        );
    }

    @Override
    public K floorKey(K key) {
        return getEntryKey(floorEntry(key));
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        if (rootObjects == null) {
            return null;
        }

        final int depth = this.depth;

        Node backtrackParent = null; // Deepest internal node on the path to "key" which has a child next to the one we descended into
        int backtrackIndex = -1;     // Index of that next child
        int backtrackDepth = -1;     // Depth of that internal node

        Node repr = rootObjects;
        for (int i = 0; i < depth; i++) {
            final int index = Internal.find(repr, key, comparator);
            if (index < repr.size - 1) {
                backtrackParent = repr;
                backtrackIndex = index + 1;
                backtrackDepth = i;
            }
            repr = Internal.getNode(repr, index);
        }

        final int leafIndex = Leaf.find(repr, key, comparator);
        final int returnIndex;
        if (leafIndex >= 0) {
            returnIndex = leafIndex;
        } else {
            final int insertionPoint = -(leafIndex + 1);
            if (insertionPoint < repr.size) {
                returnIndex = insertionPoint;
            } else {
                // insertionPoint == repr.size: we need to find the first item in the next leaf node.
                if (backtrackParent == null) {
                    // Oh -- that was the last leaf node
                    return null;
                }

                repr = backtrackParent;
                int index = backtrackIndex;
                for (int i = backtrackDepth; i < depth; i++) {
                    repr = Internal.getNode(repr, index);
                    index = 0;
                }

                returnIndex = index;
            }
        }

        return new AbstractMap.SimpleImmutableEntry<>(
            (K)Leaf.getKey  (repr, returnIndex),
            (V)Leaf.getValue(repr, returnIndex)
        );
    }

    @Override
    public K ceilingKey(K key) {
        return getEntryKey(ceilingEntry(key));
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        if (rootObjects == null) {
            return null;
        }

        final int depth = this.depth;

        Node backtrackParent = null; // Deepest internal node on the path to "key" which has a child next to the one we descended into
        int backtrackIndex = -1;     // Index of that next child
        int backtrackDepth = -1;     // Depth of that internal node

        Node repr = rootObjects;
        for (int i = 0; i < depth; i++) {
            final int index = Internal.find(repr, key, comparator);
            if (index < repr.size - 1) {
                backtrackParent = repr;
                backtrackIndex = index + 1;
                backtrackDepth = i;
            }
            repr = Internal.getNode(repr, index);
        }

        final int leafIndex = Leaf.find(repr, key, comparator);
        final int insertionPoint = leafIndex >= 0 ? leafIndex + 1 : -(leafIndex + 1);
        final int returnIndex;
        if (insertionPoint < repr.size) {
            returnIndex = insertionPoint;
        } else {
            // insertionPoint == repr.size: we need to find the first item in the next leaf node.
            if (backtrackParent == null) {
                // Oh -- that was the last leaf node
                return null;
            }

            repr = backtrackParent;
            int index = backtrackIndex;
            for (int i = backtrackDepth; i < depth; i++) {
                repr = Internal.getNode(repr, index);
                index = 0;
            }

            returnIndex = index;
        }

        return new AbstractMap.SimpleImmutableEntry<>(
            (K)Leaf.getKey  (repr, returnIndex),
            (V)Leaf.getValue(repr, returnIndex)
        );
    }

    @Override
    public K higherKey(K key) {
        return getEntryKey(higherEntry(key));
    }

    @Override
    public Entry<K, V> firstEntry() {
        if (rootObjects == null) {
            return null;
        }

        Node repr = rootObjects;
        int depth = this.depth;

        while (depth-- > 0) {
            final int index = 0;
            repr = Internal.getNode(repr, index);
        }

        final int size = repr.size;
        if (size == 0) {
            return null;
        } else {
            final int index = 0;
            return new AbstractMap.SimpleImmutableEntry<>(
                (K)Leaf.getKey  (repr, index),
                (V)Leaf.getValue(repr, index)
            );
        }
    }

    @Override
    public Entry<K, V> lastEntry() {
        if (rootObjects == null) {
            return null;
        }

        Node repr = rootObjects;
        int depth = this.depth;

        while (depth-- > 0) {
            final int index = repr.size - 1;
            repr = Internal.getNode(repr, index);
        }

        final int size = repr.size;
        if (size == 0) {
            return null;
        } else {
            final int index = size - 1;
            return new AbstractMap.SimpleImmutableEntry<>(
                (K)Leaf.getKey  (repr, index),
                (V)Leaf.getValue(repr, index)
            );
        }
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        // TODO: fast path?
        final Entry<K, V> e = firstEntry();
        if (e != null) {
            remove(e.getKey());
        }

        return e;
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        // TODO: fast path?
        final Entry<K, V> e = lastEntry();
        if (e != null) {
            remove(e.getKey());
        }

        return e;
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return new DescendingNavigableMap<K, V>(this.asNavigableMap2());
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return new NavigableMapKeySet<K>(this);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return descendingMap().navigableKeySet();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return asNavigableMap2().subMap(fromKey, fromInclusive, toKey, toInclusive).asNavigableMap();
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return asNavigableMap2().headMap(toKey, inclusive).asNavigableMap();
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return asNavigableMap2().tailMap(fromKey, inclusive).asNavigableMap();
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return headMap(toKey, false);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return tailMap(fromKey, true);
    }

    @Override
    public K firstKey() {
        final Entry<K, V> e = firstEntry();
        if (e == null) throw new NoSuchElementException();

        return e.getKey();
    }

    @Override
    public K lastKey() {
        final Entry<K, V> e = lastEntry();
        if (e == null) throw new NoSuchElementException();

        return e.getKey();
    }

    @Override
    public NavigableSet<K> keySet() {
        return navigableKeySet();
    }

    @Override
    public Collection<V> values() {
        return new MapValueCollection<>(this);
    }

    private class EntryIterator implements Iterator<Entry<K, V>> {
        // indexes[0] is an index into rootObjects.
        // indexes[i] is an index into nodes[i - 1] (for i >= 1)
        private final int[] indexes = new int[depth + 1];
        private final Node[] nodes = new Node[depth];
        // If nextLevel >= 0:
        //   1. indexes[nextLevel] < size - 1
        //   2. There is no level l > nextLevel such that indexes[l] < size - 1
        private int nextLevel;
        private boolean hasNext;

        public void positionAtFirst() {
            nextLevel = -1;
            hasNext = false;
            if (rootObjects != null) {
                Node node = rootObjects;
                for (int i = 0;; i++) {
                    final int index = indexes[i] = 0;
                    if (index < node.size - 1) {
                        nextLevel = i;
                    }

                    if (i >= nodes.length) {
                        break;
                    }

                    node = nodes[i] = Internal.getNode(node, index);
                }

                hasNext = node.size > 0;
            }
        }

        private Node findLeaf(K key) {
            if (rootObjects == null) {
                return null;
            }

            Node repr = rootObjects;
            for (int i = 0; i < nodes.length; i++) {
                final int index = indexes[i] = Internal.find(repr, key, comparator);
                if (index < repr.size - 1) {
                    nextLevel = i;
                    // backtrackParent == nextLevel == 0 ? rootObjects : nodes[nextLevel - 1]
                    // backtrackIndex  == indexes[nextLevel] + 1
                }
                repr = nodes[i] = Internal.getNode(repr, index);
            }

            return repr;
        }

        private void findNextLevel() {
            for (int i = indexes.length - 1; i >= 0; i--) {
                final Node node = i == 0 ? rootObjects : nodes[i - 1];
                if (indexes[i] < node.size - 1) {
                    nextLevel = i;
                    break;
                }
            }
        }

        private void positionAtIndex(Node repr, int returnIndex) {
            if (returnIndex >= repr.size) {
                // We need to find the first item in the next leaf node.
                int i = nextLevel;
                if (i < 0) {
                    // Oh -- that was the last leaf node
                    return;
                }

                repr = i == 0 ? rootObjects : nodes[i - 1];
                int index = indexes[i] + 1;
                for (; i < nodes.length; i++) {
                    indexes[i] = index;
                    repr = nodes[i] = Internal.getNode(repr, index);
                    index = 0;
                }

                returnIndex = index;
                nextLevel = Integer.MIN_VALUE;
            }

            hasNext = true;
            indexes[nodes.length] = returnIndex;

            // Restore the nextLevel invariant
            if (returnIndex < repr.size - 1) {
                nextLevel = nodes.length;
            } else if (nextLevel == Integer.MIN_VALUE) {
                // We already "used up" backtrackDepth
                findNextLevel();
            }
        }

        public void positionAtCeiling(K key) {
            nextLevel = -1;
            hasNext = false;

            final Node leaf = findLeaf(key);
            if (leaf == null) {
                return;
            }

            final int leafIndex = Leaf.find(leaf, key, comparator);
            positionAtIndex(leaf, leafIndex >= 0 ? leafIndex : -(leafIndex + 1));
        }

        public void positionAtHigher(K key) {
            nextLevel = -1;
            hasNext = false;

            final Node leaf = findLeaf(key);
            if (leaf == null) {
                return;
            }

            final int leafIndex = Leaf.find(leaf, key, comparator);
            positionAtIndex(leaf, leafIndex >= 0 ? leafIndex + 1 : -(leafIndex + 1));
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            final Entry<K, V> result;
            {
                final Node leafNode = nodes.length == 0 ? rootObjects : nodes[nodes.length - 1];
                final int ix = indexes[indexes.length - 1];
                result = new AbstractMap.SimpleImmutableEntry<K, V>(
                        (K)Leaf.getKey(leafNode, ix),
                        (V)Leaf.getValue(leafNode, ix)
                );
            }

            if (nextLevel < 0) {
                hasNext = false;
            } else {
                int index = ++indexes[nextLevel];
                Node node = nextLevel == 0 ? rootObjects : nodes[nextLevel - 1];
                assert index < node.size;
                if (nextLevel < nodes.length) {
                    // We stepped forward to a later item in an internal node: update all children
                    for (int i = nextLevel; i < nodes.length;) {
                        node = nodes[i++] = Internal.getNode(node, index);
                        index = indexes[i] = 0;
                    }

                    nextLevel = nodes.length;
                } else if (index == node.size - 1) {
                    // We stepped forward to the last item in a leaf node: find parent we should step forward next
                    assert nextLevel == nodes.length;
                    nextLevel = -1;
                    for (int i = nodes.length - 1; i >= 0; i--) {
                        node = i == 0 ? rootObjects : nodes[i - 1];
                        index = indexes[i];
                        if (index < node.size - 1) {
                            nextLevel = i;
                            break;
                        }
                    }
                }
            }

            return result;
        }

        @Override
        public void remove() {
            // FIXME
            throw new UnsupportedOperationException("Iterator.remove() isn't supported yet, but I wouldn't be averse to adding it.");
        }
    }

    Iterator<Entry<K, V>> firstIterator() {
        final EntryIterator it = new EntryIterator();
        it.positionAtFirst();
        return it;
    }

    Iterator<Entry<K, V>> ceilingIterator(K k) {
        final EntryIterator it = new EntryIterator();
        it.positionAtCeiling(k);
        return it;
    }

    Iterator<Entry<K, V>> higherIterator(K k) {
        final EntryIterator it = new EntryIterator();
        it.positionAtHigher(k);
        return it;
    }

    private class DescendingEntryIterator implements Iterator<Entry<K, V>> {
        // indexes[0] is an index into rootObjects.
        // indexes[i] is an index into nodes[i - 1] (for i >= 1)
        private final int[] indexes = new int[depth + 1];
        private final Node[] nodes = new Node[depth];
        // If nextLevel >= 0:
        //   1. indexes[nextLevel] > 0
        //   2. There is no level l > nextLevel such that indexes[l] > 0
        private int nextLevel;
        private boolean hasNext;

        public void positionAtLast() {
            nextLevel = -1;
            hasNext = false;
            if (rootObjects != null) {
                Node node = rootObjects;
                for (int i = 0;; i++) {
                    final int index = indexes[i] = node.size - 1;
                    if (index > 0) {
                        nextLevel = i;
                    }

                    if (i >= nodes.length) {
                        break;
                    }

                    node = nodes[i] = Internal.getNode(node, index);
                }

                hasNext = node.size > 0;
            }
        }

        private Node findLeaf(K key) {
            if (rootObjects == null) {
                return null;
            }

            Node repr = rootObjects;
            for (int i = 0; i < nodes.length; i++) {
                final int index = indexes[i] = Internal.find(repr, key, comparator);
                if (index > 0) {
                    nextLevel = i;
                }
                repr = nodes[i] = Internal.getNode(repr, index);
            }

            return repr;
        }

        private void positionAtIndex(int returnIndex) {
            if (returnIndex < 0) {
                // We need to find the last item in the prior leaf node.
                int i = nextLevel;
                if (i < 0) {
                    // Oh -- that was the first leaf node
                    return;
                }

                Node repr = i == 0 ? rootObjects : nodes[i - 1];
                int index = indexes[i] - 1;
                for (; i < nodes.length; i++) {
                    indexes[i] = index;
                    repr = nodes[i] = Internal.getNode(repr, index);
                    index = repr.size - 1;
                }

                returnIndex = index;
                nextLevel = Integer.MIN_VALUE;
            }

            hasNext = true;
            indexes[nodes.length] = returnIndex;

            // Restore the nextLevel invariant
            if (returnIndex > 0) {
                nextLevel = nodes.length;
            } else if (nextLevel == Integer.MIN_VALUE) {
                // We already "used up" backtrackDepth
                for (int i = indexes.length - 1; i >= 0; i--) {
                    if (indexes[i] > 0) {
                        nextLevel = i;
                        break;
                    }
                }
            }
        }

        public void positionAtFloor(K key) {
            nextLevel = -1;
            hasNext = false;

            final Node leaf = findLeaf(key);
            if (leaf == null) {
                return;
            }

            final int leafIndex = Leaf.find(leaf, key, comparator);
            positionAtIndex(leafIndex >= 0 ? leafIndex : -(leafIndex + 1) - 1);
        }

        public void positionAtLower(K key) {
            nextLevel = -1;
            hasNext = false;

            final Node leaf = findLeaf(key);
            if (leaf == null) {
                return;
            }

            final int leafIndex = Leaf.find(leaf, key, comparator);
            positionAtIndex(leafIndex >= 0 ? leafIndex - 1 : -(leafIndex + 1) - 1);
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext) {
                throw new NoSuchElementException();
            }

            final Entry<K, V> result;
            {
                final Node leafNode = nodes.length == 0 ? rootObjects : nodes[nodes.length - 1];
                final int ix = indexes[indexes.length - 1];
                result = new AbstractMap.SimpleImmutableEntry<K, V>(
                        (K)Leaf.getKey(leafNode, ix),
                        (V)Leaf.getValue(leafNode, ix)
                );
            }

            if (nextLevel < 0) {
                hasNext = false;
            } else {
                int index = --indexes[nextLevel];
                assert index >= 0;
                if (nextLevel < nodes.length) {
                    // We stepped back to an earlier item in an internal node: update all children
                    Node node = nextLevel == 0 ? rootObjects : nodes[nextLevel - 1];
                    for (int i = nextLevel; i < nodes.length;) {
                        node = nodes[i++] = Internal.getNode(node, index);
                        index = indexes[i] = node.size - 1;
                        assert index > 0;
                    }

                    nextLevel = nodes.length;
                } else if (index == 0) {
                    // We stepped back to the first item in a leaf node: find parent we should step back next
                    assert nextLevel == nodes.length;
                    nextLevel = -1;
                    for (int i = nodes.length - 1; i >= 0; i--) {
                        //Node node = i == 0 ? rootObjects : nodes[i - 1];
                        index = indexes[i];
                        if (index > 0) {
                            nextLevel = i;
                            break;
                        }
                    }
                }
            }

            return result;
        }

        @Override
        public void remove() {
            // FIXME
            throw new UnsupportedOperationException("Iterator.remove() isn't supported yet, but I wouldn't be averse to adding it.");
        }
    }

    Iterator<Entry<K, V>> lastIterator() {
        final DescendingEntryIterator it = new DescendingEntryIterator();
        it.positionAtLast();;
        return it;
    }

    Iterator<Entry<K, V>> lowerIterator(K key) {
        final DescendingEntryIterator it = new DescendingEntryIterator();
        it.positionAtLower(key);
        return it;
    }

    Iterator<Entry<K, V>> floorIterator(K key) {
        final DescendingEntryIterator it = new DescendingEntryIterator();
        it.positionAtFloor(key);
        return it;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new MapEntrySet<>(this, this::firstIterator);
    }

    NavigableMap2<K, V> asNavigableMap2() {
        return new NavigableMap2<K, V>() {
            @Override
            public NavigableMap<K, V> asNavigableMap() {
                return BTreeMap.this;
            }

            @Override
            public Set<Entry<K, V>> descendingEntrySet() {
                return new MapEntrySet<>(BTreeMap.this, BTreeMap.this::lastIterator);
            }

            @Override
            public NavigableMap2<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
                return new RestrictedBTreeMap<>(
                        BTreeMap.this, fromKey, toKey,
                        Bound.inclusive(fromInclusive),
                        Bound.inclusive(toInclusive)).asNavigableMap2();
            }

            @Override
            public NavigableMap2<K, V> headMap(K toKey, boolean inclusive) {
                return new RestrictedBTreeMap<>(
                        BTreeMap.this, null, toKey,
                        Bound.MISSING,
                        Bound.inclusive(inclusive)).asNavigableMap2();
            }

            @Override
            public NavigableMap2<K, V> tailMap(K fromKey, boolean inclusive) {
                return new RestrictedBTreeMap<>(
                        BTreeMap.this, fromKey, null,
                        Bound.inclusive(inclusive),
                        Bound.MISSING).asNavigableMap2();
            }
        };
    }

    @Override
    public V remove(Object key) {
        if (rootObjects == null) {
            return null;
        }

        final V result = removeCore(rootObjects, depth, key);
        if (rootObjects.size == 1 && depth > 0) {
            rootObjects = Internal.getNode(rootObjects, 0);
            depth--;
        }

        return result;
    }

    private V removeCore(Node node, int depth, Object key) {
        if (depth == 0) {
            final int index = Leaf.find(node, key, comparator);
            if (index < 0) {
                return null;
            } else {
                final V result = (V)Leaf.getValue(node, index);

                size--;
                node.size--;
                Node.arraycopy(node, Leaf.getKeyIndex(index)   + 1, node, Leaf.getKeyIndex(index),   node.size - index);
                Node.arraycopy(node, Leaf.getValueIndex(index) + 1, node, Leaf.getValueIndex(index), node.size - index);

                // Avoid memory leaks
                node.set(Leaf.getKeyIndex(node.size),   null);
                node.set(Leaf.getValueIndex(node.size), null);

                return result;
            }
        } else {
            final int index = Internal.find(node, key, comparator);
            final Node child = Internal.getNode(node, index);
            final V result = removeCore(child, depth - 1, key);

            if (child.size < Node.MIN_FANOUT) {
                assert child.size == Node.MIN_FANOUT - 1;

                if (index > 0) {
                    // Take key from or merge with predecessor
                    final Node pred = Internal.getNode(node, index - 1);
                    if (pred.size > Node.MIN_FANOUT) {
                        // Can take key from predecessor
                        final int predSize = --pred.size;
                        final int childSize = child.size++;

                        final Object predLtKey;
                        if (depth == 1) {
                            // Children are leaves
                            predLtKey = pred.get(Leaf.getKeyIndex(predSize));
                            final Object predValue = pred.get(Leaf.getValueIndex(predSize));

                            // Avoid memory leaks
                            pred.set(Leaf.getKeyIndex(predSize),   null);
                            pred.set(Leaf.getValueIndex(predSize), null);

                            Node.arraycopy(child, Leaf.getKeyIndex(0),   child, Leaf.getKeyIndex(1),   childSize);
                            Node.arraycopy(child, Leaf.getValueIndex(0), child, Leaf.getValueIndex(1), childSize);
                            child.set(Leaf.getKeyIndex(0),   predLtKey);
                            child.set(Leaf.getValueIndex(0), predValue);
                        } else {
                            // Children are internal nodes
                            predLtKey = Internal.getKey(pred, predSize - 1);
                            final Object predKey = Internal.getKey(node, index - 1);
                            final Node predNode = Internal.getNode(pred, predSize);

                            // Avoid memory leaks
                            pred.set(Internal.getKeyIndex(predSize - 1), null);
                            pred.set(Internal.getNodeIndex(predSize),    null);

                            Node.arraycopy(child, Internal.getKeyIndex(0),  child, Internal.getKeyIndex(1),  childSize - 1);
                            Node.arraycopy(child, Internal.getNodeIndex(0), child, Internal.getNodeIndex(1), childSize);
                            child.set(Internal.getKeyIndex(0),  predKey);
                            child.set(Internal.getNodeIndex(0), predNode);
                        }

                        node.set(Internal.getKeyIndex(index - 1), predLtKey);
                    } else {
                        // Can merge with predecessor
                        final Object middleKey = Internal.getKey(node, index - 1);
                        Internal.deleteAtIndex(node, index);
                        appendToPred(pred, middleKey, child, depth - 1);
                    }
                } else {
                    // Take key from or merge with successor (there must be one because all nodes except the root must have at least 1 sibling)
                    final Node succ = Internal.getNode(node, index + 1);
                    if (succ.size > Node.MIN_FANOUT) {
                        // Can take key from successor
                        final int succSize = --succ.size;
                        final int childSize = child.size++;

                        final Object succGteKey;
                        if (depth == 1) {
                            // Children are leaves
                            succGteKey = Leaf.getKey(succ, 1);
                            final Object succKey = succ.get(Leaf.getKeyIndex(0));
                            final Object succValue = succ.get(Leaf.getValueIndex(0));

                            Node.arraycopy(succ, Leaf.getKeyIndex(1),   succ, Leaf.getKeyIndex(0),   succSize);
                            Node.arraycopy(succ, Leaf.getValueIndex(1), succ, Leaf.getValueIndex(0), succSize);

                            // Avoid memory leaks
                            succ.set(Leaf.getKeyIndex(succSize),   null);
                            succ.set(Leaf.getValueIndex(succSize), null);

                            child.set(Leaf.getKeyIndex(childSize),   succKey);
                            child.set(Leaf.getValueIndex(childSize), succValue);
                        } else {
                            // Children are internal nodes
                            succGteKey = Internal.getKey(succ, 0);
                            final Object succKey = Internal.getKey(node, index);
                            final Node succNode = Internal.getNode(succ, 0);

                            Node.arraycopy(succ, Internal.getKeyIndex(1),  succ, Internal.getKeyIndex(0),  succSize - 1);
                            Node.arraycopy(succ, Internal.getNodeIndex(1), succ, Internal.getNodeIndex(0), succSize);

                            // Avoid memory leaks
                            succ.set(Internal.getKeyIndex(succSize - 1), null);
                            succ.set(Internal.getNodeIndex(succSize),    null);

                            child.set(Internal.getKeyIndex(childSize),  succKey);
                            child.set(Internal.getNodeIndex(childSize), succNode);
                        }

                        node.set(Internal.getKeyIndex(index), succGteKey);
                    } else {
                        // Can merge with successor
                        final Object middleKey = Internal.getKey(node, index);
                        Internal.deleteAtIndex(node, index + 1);
                        appendToPred(child, middleKey, succ, depth - 1);
                    }
                }
            }

            return result;
        }
    }

    private static void appendToPred(Node pred, Object middleKey, Node succ, int depth) {
        final int succSize = succ.size;
        final int predSize = pred.size;

        pred.size = predSize + succSize;
        assert pred.size == MAX_FANOUT;

        if (depth == 0) {
            // Children are leaves
            Node.arraycopy(succ, Leaf.getKeyIndex(0),   pred, Leaf.getKeyIndex(predSize),   succSize);
            Node.arraycopy(succ, Leaf.getValueIndex(0), pred, Leaf.getValueIndex(predSize), succSize);
        } else {
            // Children are internal nodes
            pred.set(Internal.getKeyIndex(predSize - 1), middleKey);
            Node.arraycopy(succ, Internal.getKeyIndex(0),  pred, Internal.getKeyIndex(predSize),  succSize - 1);
            Node.arraycopy(succ, Internal.getNodeIndex(0), pred, Internal.getNodeIndex(predSize), succSize);
        }
    }
}