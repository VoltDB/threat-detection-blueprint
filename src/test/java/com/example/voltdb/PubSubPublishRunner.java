package com.example.voltdb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.voltdb.VoltTable;
import org.voltdb.client.Client2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test that sends seed transaction data through VoltDB and publishes
 * events to PubSub for BigQuery ingestion.
 *
 * Requires:
 * - External VoltDB running on localhost:21211
 * - GCP credentials available (GOOGLE_APPLICATION_CREDENTIALS or gcloud auth)
 * - application.properties with sendToPubSub=true
 *
 * IPs are from MaxMind GeoIP2 example files for correct BigQuery geo enrichment.
 * Available countries: UK, US, Sweden, China, Australia, Singapore, Philippines.
 *
 * Run with: mvn failsafe:integration-test -Dit.test=PubSubPublishRunner
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PubSubPublishRunner extends IntegrationTestBase {

    private static final String SEED_CSV = "data/seed_transactions.csv";

    private Client2 client;
    private ThreatDetectionApp app;

    record SeedTransaction(long accountId, int merchantId, double amount,
                           String deviceId, String sourceIp, String group) {}

    @BeforeAll
    void setUp() throws Exception {
        client = createExternalClient();
        configureExternalInstance(client);
        app = new ThreatDetectionApp(client);

        CsvDataLoader loader = new CsvDataLoader();
        app.deleteAllData();
        loader.loadAccountData(app, "data/accounts.csv");
        loader.loadMerchantData(app, "data/merchants.csv");

        System.out.printf("Publisher caches: %d accounts, %d merchants%n",
            app.getTransactionPublisher().getAccountCacheSize(),
            app.getTransactionPublisher().getMerchantCacheSize());
    }

    @AfterAll
    void tearDown() throws Exception {
        if (app != null) {
            app.getTransactionPublisher().close();
        }
        if (client != null) {
            client.close();
        }
    }

    @Test
    void sendSeedData() throws Exception {
        List<SeedTransaction> seeds = loadSeedCsv();
        System.out.printf("Loaded %d seed transactions%n", seeds.size());

        // Group by timing group
        Map<String, List<SeedTransaction>> groups = new LinkedHashMap<>();
        for (SeedTransaction s : seeds) {
            groups.computeIfAbsent(s.group(), k -> new ArrayList<>()).add(s);
        }
        for (var entry : groups.entrySet()) {
            System.out.printf("  %s: %d rows%n", entry.getKey(), entry.getValue().size());
        }

        // Unique txn_id base per run
        long txnIdBase = System.currentTimeMillis() * 1000;
        long baseTime = System.currentTimeMillis();

        // Track results
        int totalSent = 0;
        int acceptedCount = 0;
        Map<String, Integer> rejectedByRule = new LinkedHashMap<>();

        // Process each group with appropriate timing
        String[] groupOrder = {"normal", "velocity_burst", "high_spend", "subnet_flood",
                               "disabled", "invalid_merchant"};

        for (String groupName : groupOrder) {
            List<SeedTransaction> groupTxns = groups.get(groupName);
            if (groupTxns == null) continue;

            // Each group gets its own time window, aligned to boundary with padding
            long groupBase = computeGroupStartTime(baseTime, groupName);

            System.out.printf("%nProcessing group: %s (%d transactions)%n", groupName, groupTxns.size());

            for (int i = 0; i < groupTxns.size(); i++) {
                SeedTransaction s = groupTxns.get(i);
                long txnId = txnIdBase + totalSent;
                long txnTime = groupBase + getIntraGroupDelay(groupName, i);

                try {
                    VoltTable result = app.processRequest(
                        s.accountId(), txnId, txnTime,
                        s.merchantId(), s.amount(), s.deviceId(), s.sourceIp());

                    result.advanceRow();
                    int accepted = (int) result.getLong("ACCEPTED");
                    String ruleName = result.getString("RULE_NAME");

                    if (accepted == 1) {
                        acceptedCount++;
                    } else {
                        rejectedByRule.merge(ruleName, 1, Integer::sum);
                    }
                    totalSent++;
                } catch (Exception e) {
                    System.err.printf("  ERROR txn %d (account=%d, ip=%s): %s%n",
                        txnId, s.accountId(), s.sourceIp(), e.getMessage());
                    totalSent++;
                }
            }
        }

        // Flush PubSub
        int flushed = app.getTransactionPublisher().flush();

        // Print report
        int totalRejected = rejectedByRule.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println();
        System.out.println("========================================");
        System.out.println("SEED DATA REPORT");
        System.out.println("========================================");
        System.out.printf("Total sent:        %d%n", totalSent);
        System.out.printf("  Accepted:        %d%n", acceptedCount);
        System.out.printf("  Rejected:        %d%n", totalRejected);
        for (var entry : rejectedByRule.entrySet()) {
            System.out.printf("    %-25s %d%n", entry.getKey() + ":", entry.getValue());
        }
        System.out.printf("PubSub flushed:    %d%n", flushed);
        System.out.println("========================================");
    }

    // ========================================
    // Timing configuration per group
    // ========================================

    /**
     * Compute the start timestamp for a group, aligned to a TIME_WINDOW boundary
     * with padding to ensure all transactions land within a single window.
     *
     * VoltDB TIME_WINDOW creates tumbling windows aligned to epoch.
     * E.g., TIME_WINDOW(SECOND, 30, ...) creates windows at [:00-:30), [:30-:60).
     * If transactions straddle a boundary, they split across two windows and
     * may not trigger the threshold. To avoid this, we:
     * 1. Align to the start of a window boundary
     * 2. Add padding (2 seconds) so the first transaction is safely inside the window
     * 3. Ensure all transactions in the group fit within the remaining window time
     */
    private long computeGroupStartTime(long baseTime, String group) {
        long offset = switch (group) {
            case "normal"           -> 0;
            case "velocity_burst"   -> 720_000;        // +12 minutes
            case "high_spend"       -> 900_000;        // +15 minutes
            case "subnet_flood"     -> 1_080_000;      // +18 minutes
            case "disabled"         -> 1_200_000;      // +20 minutes
            case "invalid_merchant" -> 1_200_000;      // +20 minutes
            default                 -> 1_300_000;
        };
        long rawTime = baseTime + offset;

        long windowMs = switch (group) {
            case "velocity_burst" -> 30_000L;   // 30s window
            case "high_spend"     -> 60_000L;   // 60s window
            case "subnet_flood"   -> 5_000L;    // 5s window
            default               -> 0L;
        };

        if (windowMs > 0) {
            // Align to the next window boundary, then add padding
            long nextBoundary = ((rawTime / windowMs) + 1) * windowMs;
            long padding = (group.equals("subnet_flood")) ? 500 : 2000;
            return nextBoundary + padding;
        }
        return rawTime;
    }

    private long getIntraGroupDelay(String group, int index) {
        return switch (group) {
            case "normal"         -> index * 3500L;    // ~3.5s apart over ~5 min
            case "velocity_burst" -> index * 500L;     // 500ms apart, 40 txns = 20s (fits in 30s window)
            case "high_spend"     -> index * 2000L;    // 2s apart, 24 txns = 48s (fits in 60s window)
            case "subnet_flood"   -> index * 13L;      // 13ms apart, 315 txns = 4.1s (fits in 5s window)
            default               -> index * 1000L;
        };
    }

    // ========================================
    // CSV loading
    // ========================================

    private List<SeedTransaction> loadSeedCsv() throws Exception {
        List<SeedTransaction> transactions = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getResource(SEED_CSV)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith("account_id")) {
                    continue; // skip comments and header
                }
                String[] fields = parseCsvLine(line);
                transactions.add(new SeedTransaction(
                    Long.parseLong(fields[0].trim()),
                    Integer.parseInt(fields[1].trim()),
                    Double.parseDouble(fields[2].trim()),
                    fields[3].trim(),
                    fields[4].trim(),
                    fields[5].trim()
                ));
            }
        }
        return transactions;
    }

    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }

    private InputStream getResource(String resourcePath) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) {
            throw new RuntimeException("Resource not found: " + resourcePath);
        }
        return is;
    }
}
