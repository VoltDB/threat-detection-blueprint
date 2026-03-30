package com.example.voltdb;

import org.voltdb.VoltTable;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Threat Detection VoltDB client application.
 * Combines fraud transaction detection with CIDR subnet-based threat detection.
 *
 * Flow for each incoming transaction:
 * 1. Extract subnet from source IP using the configured CIDR prefix length
 * 2. Call RecordSubnetRequest to track subnet activity and get current count
 * 3. Call ProcessTransaction with the subnet count to evaluate all threat rules
 * 4. Publish the transaction event to PubSub for BigQuery export (if enabled)
 */
public class ThreatDetectionApp {

    private static final int DEFAULT_CIDR_PREFIX = 24;
    private static final String DEFAULT_GCP_PROJECT = "";
    private static final String DEFAULT_TOPIC = "threat-transactions";

    private final Client2 client;
    private final int cidrPrefix;
    private final TransactionPublisher txnPublisher;

    public ThreatDetectionApp(Client2 client) throws IOException {
        this(client, Integer.getInteger("cidr.prefix", DEFAULT_CIDR_PREFIX));
    }

    public ThreatDetectionApp(Client2 client, int cidrPrefix) throws IOException {
        if (cidrPrefix < 0 || cidrPrefix > 32) {
            throw new IllegalArgumentException("CIDR prefix must be 0-32, got: " + cidrPrefix);
        }
        this.client = client;
        this.cidrPrefix = cidrPrefix;

        // Load application.properties from classpath, then allow system properties to override
        Properties props = loadProperties();
        String gcpProject = getProperty(props, "gcp.project", DEFAULT_GCP_PROJECT);
        String topicId = getProperty(props, "pubsub.topic.transactions", DEFAULT_TOPIC);
        boolean sendToPubSub = Boolean.parseBoolean(
            getProperty(props, "sendToPubSub", "false"));

        this.txnPublisher = new TransactionPublisher(gcpProject, topicId, sendToPubSub);
    }

    /**
     * Load application.properties from classpath.
     */
    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = ThreatDetectionApp.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            // Use defaults
        }
        return props;
    }

    /**
     * Get a property value: system property takes precedence over application.properties.
     */
    private static String getProperty(Properties props, String key, String defaultValue) {
        String sysValue = System.getProperty(key);
        if (sysValue != null) {
            return sysValue;
        }
        return props.getProperty(key, defaultValue);
    }

    public int getCidrPrefix() {
        return cidrPrefix;
    }

    public TransactionPublisher getTransactionPublisher() {
        return txnPublisher;
    }

    // ========================================
    // Core: Two-step threat detection
    // ========================================

    /**
     * Record a subnet request and return the current count for the subnet.
     * This is called before ProcessTransaction to track subnet activity.
     *
     * @return the current request count for the subnet
     */
    public long recordSubnetRequest(String subnet, long requestId, String sourceIp, long requestTimeMs)
            throws Exception {
        VoltTable result = client.callProcedureAsync("RecordSubnetRequest",
                subnet, requestId, sourceIp, requestTimeMs)
            .thenApply(response -> checkResponse("RecordSubnetRequest", response).getResults()[0])
            .get();
        result.advanceRow();
        return result.getLong("REQUEST_COUNT");
    }

    /**
     * Process a transaction through all threat detection rules.
     * The subnetRequestCount should be obtained from a prior recordSubnetRequest call.
     *
     * Returns a VoltTable with columns: ACCEPTED (tinyint), REASON (string), RULE_NAME (string).
     */
    public VoltTable processTransaction(long accountId, long txnId, long txnTimeMs,
                                        int merchantId, double amount, String deviceId,
                                        String sourceIp, long subnetRequestCount)
            throws Exception {
        return client.callProcedureAsync("ProcessTransaction",
                accountId, txnId, txnTimeMs, merchantId, amount, deviceId,
                sourceIp, subnetRequestCount)
            .thenApply(response -> checkResponse("ProcessTransaction", response).getResults()[0])
            .get();
    }

    /**
     * Convenience method: records subnet request, processes transaction, and
     * publishes the event to PubSub (if enabled).
     * Extracts the subnet from sourceIp using the configured CIDR prefix length.
     */
    public VoltTable processRequest(long accountId, long txnId, long txnTimeMs,
                                    int merchantId, double amount, String deviceId,
                                    String sourceIp) throws Exception {
        String subnet = CidrUtils.extractSubnet(sourceIp, cidrPrefix);
        long subnetCount = recordSubnetRequest(subnet, txnId, sourceIp, txnTimeMs);
        VoltTable result = processTransaction(accountId, txnId, txnTimeMs, merchantId,
                                              amount, deviceId, sourceIp, subnetCount);

        // Publish to PubSub after VoltDB TX completes (non-blocking)
        result.advanceRow();
        int accepted = (int) result.getLong("ACCEPTED");
        String reason = result.getString("REASON");
        String ruleName = result.getString("RULE_NAME");
        result.resetRowPosition();

        txnPublisher.publish(txnId, accountId, txnTimeMs, merchantId, amount,
            deviceId, sourceIp, accepted, reason, ruleName);

        return result;
    }

    // ========================================
    // CRUD Operations
    // ========================================

    public void upsertAccount(long accountId, byte enabled, double balance,
                              double dailyLimit, String name, String email) throws Exception {
        client.callProcedureAsync("UpsertAccount", accountId, enabled, balance,
                dailyLimit, name, email)
            .thenApply(response -> checkResponse("UpsertAccount", response))
            .get();
    }

    public VoltTable getAccount(long accountId) throws Exception {
        return client.callProcedureAsync("GetAccount", accountId)
            .thenApply(response -> checkResponse("GetAccount", response).getResults()[0])
            .get();
    }

    public void upsertMerchant(int merchantId, String name, String category) throws Exception {
        client.callProcedureAsync("UpsertMerchant", merchantId, name, category)
            .thenApply(response -> checkResponse("UpsertMerchant", response))
            .get();
    }

    // ========================================
    // Query Operations
    // ========================================

    public VoltTable getTransactionsByAccount(long accountId) throws Exception {
        return client.callProcedureAsync("GetTransactionsByAccount", accountId)
            .thenApply(response -> checkResponse("GetTransactionsByAccount", response).getResults()[0])
            .get();
    }

    /**
     * Multi-partition search: find all blocked transactions by rule name.
     */
    public VoltTable searchBlockedByRule(String ruleName) throws Exception {
        return client.callProcedureAsync("SearchBlockedByRule", ruleName)
            .thenApply(response -> checkResponse("SearchBlockedByRule", response).getResults()[0])
            .get();
    }

    /**
     * Multi-partition search: find all blocked transactions by source IP.
     */
    public VoltTable searchBlockedByIp(String sourceIp) throws Exception {
        return client.callProcedureAsync("SearchBlockedByIp", sourceIp)
            .thenApply(response -> checkResponse("SearchBlockedByIp", response).getResults()[0])
            .get();
    }

    // ========================================
    // Cleanup
    // ========================================

    public void deleteAllData() throws Exception {
        client.callProcedureAsync("@AdHoc", "DELETE FROM TRANSACTIONS;")
            .thenCompose(r -> client.callProcedureAsync("@AdHoc", "DELETE FROM SUBNET_REQUESTS;"))
            .thenCompose(r -> client.callProcedureAsync("@AdHoc", "DELETE FROM ACCOUNTS;"))
            .thenCompose(r -> client.callProcedureAsync("@AdHoc", "DELETE FROM MERCHANTS;"))
            .get();
        System.out.println("All data deleted.");
    }

    // ========================================
    // Utility
    // ========================================

    public static void printTable(String label, VoltTable table) {
        System.out.println("\n--- " + label + " ---");
        System.out.println(table.toFormattedString());
    }

    private static ClientResponse checkResponse(String procName, ClientResponse response) {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new RuntimeException(procName + " failed: " + response.getStatusString());
        }
        return response;
    }

    // ========================================
    // Main entry point
    // ========================================

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 21211;

        Client2Config config = new Client2Config();
        Client2 client = ClientFactory.createClient(config);
        client.connectSync(host, port);
        System.out.println("Connected to VoltDB at " + host + ":" + port);

        ThreatDetectionApp app = new ThreatDetectionApp(client);
        try {
            new VoltDBSetup(client).initSchemaIfNeeded();

            CsvDataLoader loader = new CsvDataLoader();

            app.deleteAllData();

            // 1. Load reference data (also populates publisher caches)
            loader.loadAccountData(app, "data/accounts.csv");
            loader.loadMerchantData(app, "data/merchants.csv");

            // 2. Process a normal transaction (two-step: subnet tracking + fraud detection)
            //    If sendToPubSub=true, the event is published after the VoltDB TX completes.
            long now = System.currentTimeMillis();
            VoltTable result = app.processRequest(
                1, 1001, now, 1, 200.0, "device-abc", "192.168.1.45");
            result.advanceRow();
            System.out.printf("ProcessTransaction result: ACCEPTED=%d, RULE=%s, REASON=%s%n",
                result.getLong("ACCEPTED"),
                result.getString("RULE_NAME"),
                result.getString("REASON"));

            // 3. Query transaction history
            printTable("Transactions for Account 1",
                app.getTransactionsByAccount(1));

            // 4. Search blocked transactions by rule
            printTable("Blocked by TXN_SUMMARY_30SEC",
                app.searchBlockedByRule("TXN_SUMMARY_30SEC"));

            // 5. Search blocked transactions by IP
            printTable("Blocked from IP 192.168.1.45",
                app.searchBlockedByIp("192.168.1.45"));

            // 6. Flush pending PubSub messages
            int flushed = app.getTransactionPublisher().flush();
            System.out.printf("Flushed %d PubSub messages.%n", flushed);

            app.deleteAllData();

        } finally {
            app.getTransactionPublisher().close();
            client.close();
        }
    }
}
