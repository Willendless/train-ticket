package edu.fudan.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.HashMap;

public class ConsistencyCheckedCache<K, T, V> extends LinkedHashMap<K, V> {

    private String name;
    private boolean logging;
    // currently cacheSize is not used
    private int cacheSize;
    private int queryCount;
    private int hitCount;
    private int coldMiss;
    private int invalidateCount;
    private int invalidateRedundant;
    // Key, extra argument, value
    private BiFunction<K, T, V> getter;
    // request id to cached keys mapping
    private HashMap<String, K> idTokeys = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckedCache.class);

    public ConsistencyCheckedCache(String name, int cacheSize, boolean doLogging, BiFunction<K, T, V> getter) {
        super(16, 0.75f, true);
        this.name = name;
        this.cacheSize = cacheSize;
        this.getter = getter;
        this.logging = doLogging;
        queryCount = 0;
        hitCount = 0;
        invalidateCount = 0;
        invalidateRedundant = 0;
        coldMiss = 0;
    }

    public V getOrInsert(String id, K key, T extraArg) {
        long t0 = System.nanoTime();
        V result = getter.apply(key, extraArg);
        long t1 = System.nanoTime();
        V cached = get(key);
        long t2 = System.nanoTime();
        printLog("time period 1: " + (t1 - t0) + ", time period 2: " + (t2 - t1), false);

        queryCount += 1;

        if (cached == null) {
            coldMiss += 1;

            printLog("insert key: " + key + ", value: " + result, false);

            idTokeys.put(id, key);
            put(key, result);

            printLog("", true);

            return result;
        } else {

            if (cached.equals(result)) {
                hitCount += 1;
                printLog("cache hit!!", false);
            } else {
                printLog("cache miss, key: " + key + ", cached value: " + cached + ", actual value: " + result, false);
                put(key, result);
            }

            idTokeys.put(id, key);
            printLog("", true);
            return cached;
        }
    }

    private void printLog(String s, Boolean logMetrics) {
        String logString = "[" + name + "] " + s;
        if (logMetrics) {
            logString += "query count: " + queryCount + " "
            + "hit count: " + hitCount + " "
            + "cold miss: " + coldMiss + " "
            + "invalidate count: " + invalidateCount + " "
            + "redundant invalidation count: " + invalidateRedundant;
        }
        if (logging) {
            ConsistencyCheckedCache.LOGGER.info(logString);
        }
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }

    public void invalidate(String id, K key, T extraArg, boolean forward) {
        invalidateCount += 1;

        if (idTokeys.get(id) == null) {
            invalidateRedundant += 1;
            return;
        }

        printLog("!!invalidate!! " + idTokeys.get(id), true);

        remove(idTokeys.get(id));
        idTokeys.remove(id);

        if (forward) {
            getter.apply(key, extraArg);
        }
    }
}
