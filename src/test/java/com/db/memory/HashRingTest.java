package com.db.memory;

import com.db.memory.hashing.HashRing;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HashRingTest {

    @Test
    void hashIsDeterministic() {
        HashRing ring = new HashRing(null);
        int h1 = ring.hash("apple");
        int h2 = ring.hash("apple");
        assertEquals(h1, h2, "Same key should always produce the same hash");
    }

    @Test
    void differentKeysProduceDifferentHashes() {
        HashRing ring = new HashRing(null);
        int h1 = ring.hash("apple");
        int h2 = ring.hash("banana");
        assertNotEquals(h1, h2, "Different keys should generally produce different hashes");
    }

    @Test
    void hashIsNonNegative() {
        HashRing ring = new HashRing(null);
        for (String key : new String[]{"a", "b", "test", "foo", "bar", "12345"}) {
            assertTrue(ring.hash(key) >= 0, "Hash for '" + key + "' should be non-negative");
        }
    }
}
