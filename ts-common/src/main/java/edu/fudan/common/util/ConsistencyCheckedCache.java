package edu.fudan.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class ConsistencyCheckedCache<K, T, V> extends LinkedHashMap<K, V> {

    private String name;
    // currently cacheSize is not used
    private int cacheSize;
    private int queryCount;
    private int hitCount;
    private int coldMiss;
    // Key, extra argument, value
    private BiFunction<K, T, V> getter;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyCheckedCache.class);

    public ConsistencyCheckedCache(String name, int cacheSize, BiFunction<K, T, V> getter) {
        super(16, 0.75f, true);
        this.name = name;
        this.cacheSize = cacheSize;
        this.getter = getter;
        queryCount = 0;
        hitCount = 0;
        coldMiss = 0;
    }

    public V getOrInsert(K key, T extraArg) {
        V result = getter.apply(key, extraArg);
        V cached = get(key);

        queryCount += 1;

        if (cached == null) {
            coldMiss += 1;

            ConsistencyCheckedCache.LOGGER.info(
                    "[" + name + "] inserting entry for key "
                            + key
                            + ", value was "
                            + result);
            put(key, result);

            printMetrics();

            return result;
        } else {
            if (cached.equals(result)) {
                hitCount += 1;

                ConsistencyCheckedCache.LOGGER.info(
                        "[" + name + "] had a good cached entry for key "
                                + key
                                + ", value was "
                                + cached);
            } else {
                ConsistencyCheckedCache.LOGGER.info(
                        "[" + name + "] had an inconsistent cached entry for key "
                                + key
                                + ", cached value was "
                                + cached
                                + ", actual value was "
                                + result);
                put(key, result);
            }

            printMetrics();

            return cached;
        }
    }

    private void printMetrics() {
        ConsistencyCheckedCache.LOGGER.info(
            "[query count] " + queryCount + " "
          + "[hit count] " + hitCount + " "
          + "[cold miss] " + coldMiss);
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() >= cacheSize;
    }
}
