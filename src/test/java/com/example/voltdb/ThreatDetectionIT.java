package com.example.voltdb;

import org.junit.jupiter.api.Test;
import org.voltdb.VoltTable;
import org.voltdb.client.Client2;
import org.voltdbtest.testcontainer.VoltDBCluster;

import java.util.List;

import static com.example.voltdb.CidrUtils.extractSubnet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ThreatDetectionIT extends IntegrationTestBase {

    @Test
    public void testThreatDetection() {
        VoltDBCluster db = null;
        try {
            // ============================================
            // Setup: start VoltDB and deploy schema
            // ============================================
            Client2 client;
            if (isTestContainerMode()) {
                db = createTestContainer();
                startAndConfigureTestContainer(db);
                client = db.getClient2();
            } else {
                client = createExternalClient();
                configureExternalInstance(client);
            }

            ThreatDetectionApp app = new ThreatDetectionApp(client);
            CsvDataLoader loader = new CsvDataLoader();

            // ============================================
            // Load reference data
            // ============================================
            List<Long> accounts = loader.loadAccountData(app, "data/accounts.csv");
            assertEquals(5, accounts.size(), "All accounts should be loaded");

            List<Integer> merchants = loader.loadMerchantData(app, "data/merchants.csv");
            assertEquals(5, merchants.size(), "All merchants should be loaded");

            // ============================================
            // Test 1: Normal transaction — should be ACCEPTED
            // ============================================
            long now = System.currentTimeMillis();
            VoltTable result = app.processRequest(
                1, 1001, now, 1, 200.0, "device-abc", "192.168.1.45");
            assertTrue(result.advanceRow(), "Should return a result row");
            assertEquals(1L, result.getLong("ACCEPTED"),
                "First transaction should be ACCEPTED");
            assertEquals("NONE", result.getString("RULE_NAME"));

            // ============================================
            // Test 2: Invalid account — should be rejected
            // ============================================
            result = app.processRequest(
                999, 1002, now, 1, 100.0, "device-xyz", "10.0.0.1");
            assertTrue(result.advanceRow());
            assertEquals(0L, result.getLong("ACCEPTED"),
                "Invalid account should be rejected");
            assertEquals("VALIDATION", result.getString("RULE_NAME"));

            // ============================================
            // Test 3: Disabled account — should be rejected
            // ============================================
            result = app.processRequest(
                4, 1003, now, 1, 100.0, "device-xyz", "10.0.0.2");
            assertTrue(result.advanceRow());
            assertEquals(0L, result.getLong("ACCEPTED"),
                "Disabled account should be rejected");
            assertTrue(result.getString("REASON").contains("Disabled"));

            // ============================================
            // Test 4: TXN_SUMMARY_30SEC — >5 txns in 30 seconds
            // Send 6 transactions from same account within the same second.
            // The 7th should trigger the rule (>5 after insert).
            // ============================================
            long t = System.currentTimeMillis();
            for (int i = 0; i < 5; i++) {
                result = app.processTransaction(
                    2, 2000 + i, t, 1, 100.0, "device-flood",
                    "10.1.1." + i, 0);
                assertTrue(result.advanceRow());
                assertEquals(1L, result.getLong("ACCEPTED"),
                    "Transaction " + i + " should be accepted");
            }
            // The 6th transaction: after insert, view shows count=6 which is >5
            result = app.processTransaction(
                2, 2005, t, 1, 100.0, "device-flood", "10.1.1.5", 0);
            assertTrue(result.advanceRow());
            assertEquals(0L, result.getLong("ACCEPTED"),
                "6th transaction in 30 seconds should be REJECTED");
            assertEquals("TXN_SUMMARY_30SEC", result.getString("RULE_NAME"));

            // Verify rejected transaction is in TRANSACTIONS with ACCEPTED=0
            VoltTable txns = app.getTransactionsByAccount(2);
            boolean foundBlocked = false;
            while (txns.advanceRow()) {
                if (txns.getLong("ACCEPTED") == 0
                        && "TXN_SUMMARY_30SEC".equals(txns.getString("RULE_NAME"))) {
                    foundBlocked = true;
                    break;
                }
            }
            assertTrue(foundBlocked,
                "Should find a rejected transaction with TXN_SUMMARY_30SEC in TRANSACTIONS");

            // ============================================
            // Test 5: TXN_SUMMARY_1MIN — >$5,000 spent in 1 minute
            // ============================================
            long t2 = System.currentTimeMillis();
            // First transaction: $4,900 — accepted
            result = app.processTransaction(
                3, 3001, t2, 2, 4900.0, "device-spend", "10.2.2.1", 0);
            assertTrue(result.advanceRow());
            assertEquals(1L, result.getLong("ACCEPTED"),
                "$4,900 transaction should be accepted");

            // Second transaction: $200 — total becomes $5,100 which is >$5,000
            result = app.processTransaction(
                3, 3002, t2, 2, 200.0, "device-spend", "10.2.2.1", 0);
            assertTrue(result.advanceRow());
            assertEquals(0L, result.getLong("ACCEPTED"),
                "Total >$5,000 in 1 minute should be REJECTED");
            assertEquals("TXN_SUMMARY_1MIN", result.getString("RULE_NAME"));

            // ============================================
            // Test 6: SUBNET_RATE — >100 requests from same /24 subnet
            // Use extractSubnet to compute the /24 network address from any IP in the range.
            // ============================================
            long t3 = System.currentTimeMillis();
            String subnet = extractSubnet("10.50.50.1", 24); // "10.50.50.0"
            // Send 100 requests from the same /24 subnet (10.50.50.x)
            for (int i = 0; i < 100; i++) {
                String ip = "10.50.50." + (i % 254 + 1);
                app.recordSubnetRequest(subnet, 4000 + i, ip, t3);
            }
            // The next request from the same subnet should have count=101, triggering the rule
            long subnetCount = app.recordSubnetRequest(subnet, 4100, "10.50.50.99", t3);
            assertTrue(subnetCount > 100,
                "Subnet count should be >100, got: " + subnetCount);

            result = app.processTransaction(
                5, 4100, t3, 1, 50.0, "device-subnet", "10.50.50.99", subnetCount);
            assertTrue(result.advanceRow());
            assertEquals(0L, result.getLong("ACCEPTED"),
                "Subnet rate >100 should be REJECTED");
            assertEquals("SUBNET_RATE", result.getString("RULE_NAME"));

            // ============================================
            // Test 7: Multi-partition search — blocked by rule name
            // ============================================
            VoltTable ruleSearch = app.searchBlockedByRule("TXN_SUMMARY_30SEC");
            int ruleCount = 0;
            while (ruleSearch.advanceRow()) {
                ruleCount++;
            }
            assertTrue(ruleCount >= 1,
                "Should find at least 1 blocked transaction by TXN_SUMMARY_30SEC");

            // ============================================
            // Test 8: Multi-partition search — blocked by IP
            // ============================================
            VoltTable ipSearch = app.searchBlockedByIp("10.50.50.99");
            assertTrue(ipSearch.advanceRow(),
                "Should find blocked transaction for IP 10.50.50.99");
            assertEquals("SUBNET_RATE", ipSearch.getString("RULE_NAME"));

            // ============================================
            // Test 9: Transaction history includes both accepted and blocked
            // ============================================
            VoltTable txHistory = app.getTransactionsByAccount(2);
            int totalCount = 0;
            int acceptedCount = 0;
            int blockedCount = 0;
            while (txHistory.advanceRow()) {
                totalCount++;
                if (txHistory.getLong("ACCEPTED") == 1) {
                    acceptedCount++;
                } else {
                    blockedCount++;
                }
            }
            assertTrue(totalCount >= 6, "Account 2 should have at least 6 transactions");
            assertEquals(5, acceptedCount, "Account 2 should have 5 accepted transactions");
            assertTrue(blockedCount >= 1, "Account 2 should have at least 1 blocked transaction");

            // ============================================
            // Cleanup
            // ============================================
            app.deleteAllData();

            VoltTable afterCleanup = app.getTransactionsByAccount(2);
            assertFalse(afterCleanup.advanceRow(), "No data should remain after cleanup");

            System.out.println("\n*** ALL TESTS PASSED ***\n");

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            shutdownIfNeeded(db);
        }
    }
}
