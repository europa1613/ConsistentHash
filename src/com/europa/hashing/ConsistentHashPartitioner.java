package com.europa.hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashPartitioner {
    private final TreeMap<Long, String> ring;
    private final int partitions;
    private final MessageDigest md;

    private final MD5Hash md5Hash = new MD5Hash();

    public ConsistentHashPartitioner(int numberOfReplicas) throws NoSuchAlgorithmException {
        this.ring = new TreeMap<>();
        this.partitions = numberOfReplicas;
        this.md = MessageDigest.getInstance("MD5");
    }

    public static void main(String[] args) throws NoSuchAlgorithmException {
        ConsistentHashPartitioner ch = new ConsistentHashPartitioner(3);
        ch.addPartition("0");
        ch.addPartition("1");
        ch.addPartition("2");


        System.out.println("key1: is present on partition: " + ch.getPartition("key1"));
        System.out.println("key2: is present on partition: " + ch.getPartition("key2"));
        System.out.println("key3: is present on partition: " + ch.getPartition("key3"));
        System.out.println("F12346: is present on partition: " + ch.getPartition("F12346"));
        System.out.println("F12345: is present on partition: " + ch.getPartition("F12345"));
        System.out.println("sdfdsafsdgdsggdgsdg: is present on partition: " + ch.getPartition("sdfdsafsdgdsggdgsdg"));
        System.out.println("key67890: is present on partition: " + ch.getPartition("key67890"));
        System.out.println("key1: is present on partition: " + ch.getPartition("key1"));
        System.out.println("key2: is present on partition: " + ch.getPartition("key2"));
        System.out.println("key3: is present on partition: " + ch.getPartition("key3"));
        System.out.println("F12346: is present on partition: " + ch.getPartition("F12346"));
        System.out.println("H123468: is present on partition: " + ch.getPartition("H123468"));
        System.out.println("D123468: is present on partition: " + ch.getPartition("D123468"));
    }

    public void addPartition(String partition) {
        for (int i = 0; i < partitions; i++) {
            long hash = md5Hash.hash(partition + i);
            ring.put(hash, partition);
        }
    }

    public void removePartition(String partition) {
        for (int i = 0; i < partitions; i++) {
            long hash = md5Hash.hash(partition + i);
            ring.remove(hash);
        }
    }

    public String getPartition(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = md5Hash.hash(key);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, String> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private long generateHash(String key) {
        md.reset();
        md.update(key.getBytes());
        byte[] digest = md.digest();
        return ((long) (digest[3] & 0xFF) << 24) | ((long) (digest[2] & 0xFF) << 16) | ((long) (digest[1] & 0xFF) << 8) | ((long) (digest[0] & 0xFF));
    }
}