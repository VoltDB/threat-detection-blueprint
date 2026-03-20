package com.example.voltdb;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes transaction events to Google Cloud PubSub in real time as plain JSON.
 *
 * Each transaction processed by VoltDB is published as a single PubSub message
 * after the VoltDB transaction completes. Account and merchant names are resolved
 * from in-memory caches for denormalization.
 */
public class TransactionPublisher {

    private final Publisher publisher;
    private final boolean sendToPubSub;
    private final Map<Long, AccountInfo> accountCache = new ConcurrentHashMap<>();
    private final Map<Integer, MerchantInfo> merchantCache = new ConcurrentHashMap<>();
    private final List<ApiFuture<String>> pendingFutures = new ArrayList<>();

    public record AccountInfo(String name) {}
    public record MerchantInfo(String name, String category) {}

    public TransactionPublisher(String gcpProject, String topicId, boolean sendToPubSub)
            throws IOException {
        this.sendToPubSub = sendToPubSub;
        TopicName topicName = TopicName.of(gcpProject, topicId);
        this.publisher = Publisher.newBuilder(topicName).build();
        System.out.printf("TransactionPublisher initialized: topic=%s, sendToPubSub=%s%n",
            topicName, sendToPubSub);
    }

    // ========================================
    // Cache management
    // ========================================

    public void cacheAccount(long accountId, String name) {
        accountCache.put(accountId, new AccountInfo(name));
    }

    public void cacheMerchant(int merchantId, String name, String category) {
        merchantCache.put(merchantId, new MerchantInfo(name, category));
    }

    public int getAccountCacheSize() {
        return accountCache.size();
    }

    public int getMerchantCacheSize() {
        return merchantCache.size();
    }

    // ========================================
    // Publishing
    // ========================================

    /**
     * Publish a transaction event to PubSub as plain JSON.
     * Called after the VoltDB transaction has completed.
     * If sendToPubSub is false, returns null without sending.
     *
     * @return a future that completes when PubSub acknowledges the message, or null if disabled
     */
    public ApiFuture<String> publish(long txnId, long accountId, long txnTimeMs,
                                     int merchantId, double amount, String deviceId,
                                     String sourceIp, int accepted, String reason,
                                     String ruleName) {
        if (!sendToPubSub) {
            return null;
        }

        AccountInfo account = accountCache.get(accountId);
        MerchantInfo merchant = merchantCache.get(merchantId);

        String json = buildJson(txnId, accountId,
            account != null ? account.name() : null,
            txnTimeMs * 1000L, // convert ms to microseconds
            merchantId,
            merchant != null ? merchant.name() : null,
            merchant != null ? merchant.category() : null,
            amount, deviceId, sourceIp, accepted, reason, ruleName);

        PubsubMessage message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(json, StandardCharsets.UTF_8))
            .build();

        ApiFuture<String> future = publisher.publish(message);
        synchronized (pendingFutures) {
            pendingFutures.add(future);
        }
        return future;
    }

    /**
     * Wait for all pending publishes to complete.
     *
     * @return the number of messages flushed
     */
    public int flush() {
        List<ApiFuture<String>> toFlush;
        synchronized (pendingFutures) {
            toFlush = new ArrayList<>(pendingFutures);
            pendingFutures.clear();
        }
        int count = 0;
        for (ApiFuture<String> future : toFlush) {
            try {
                future.get();
                count++;
            } catch (Exception e) {
                System.err.println("PubSub publish failed: " + e.getMessage());
            }
        }
        return count;
    }

    public void close() {
        flush();
        publisher.shutdown();
        System.out.println("TransactionPublisher closed.");
    }

    // ========================================
    // Plain JSON serialization
    // ========================================

    private static String buildJson(long txnId, long accountId, String accountName,
                                    long txnTimeMicros, int merchantId,
                                    String merchantName, String merchantCategory,
                                    double amount, String deviceId, String sourceIp,
                                    int accepted, String reason, String ruleName) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        appendLong(sb, "txn_id", txnId); sb.append(',');
        appendLong(sb, "account_id", accountId); sb.append(',');
        appendString(sb, "account_name", accountName); sb.append(',');
        appendLong(sb, "txn_time", txnTimeMicros); sb.append(',');
        appendInt(sb, "merchant_id", merchantId); sb.append(',');
        appendString(sb, "merchant_name", merchantName); sb.append(',');
        appendString(sb, "merchant_category", merchantCategory); sb.append(',');
        appendDouble(sb, "amount", amount); sb.append(',');
        appendString(sb, "device_id", deviceId); sb.append(',');
        appendString(sb, "source_ip", sourceIp); sb.append(',');
        appendInt(sb, "accepted", accepted); sb.append(',');
        appendString(sb, "reason", reason); sb.append(',');
        appendString(sb, "rule_name", ruleName);
        sb.append('}');
        return sb.toString();
    }

    private static void appendString(StringBuilder sb, String key, String value) {
        sb.append('"').append(key).append("\":");
        if (value == null) {
            sb.append("null");
        } else {
            sb.append('"').append(escapeJson(value)).append('"');
        }
    }

    private static void appendLong(StringBuilder sb, String key, long value) {
        sb.append('"').append(key).append("\":").append(value);
    }

    private static void appendInt(StringBuilder sb, String key, int value) {
        sb.append('"').append(key).append("\":").append(value);
    }

    private static void appendDouble(StringBuilder sb, String key, double value) {
        sb.append('"').append(key).append("\":").append(value);
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
