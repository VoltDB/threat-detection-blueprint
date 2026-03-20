package com.example.voltdb.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import java.math.BigDecimal;

/**
 * Multi-step atomic procedure: Process a financial transaction for threat detection.
 * Partitioned on ACCOUNT_ID — all transactions for the same account are on the same partition.
 *
 * Combines fraud detection (per-account velocity rules) with subnet-based threat detection.
 * The subnet request count is passed in from a prior call to RecordSubnetRequest.
 *
 * Steps (all execute as a single ACID transaction):
 * 1. Validate account (exists, enabled) and merchant (exists)
 * 2. Insert the transaction record
 * 3. Check TXN_SUMMARY_1MIN: reject if >$5,000 spent in 1 minute
 * 4. Check TXN_SUMMARY_30SEC: reject if >5 txns in 30 seconds
 * 5. Check subnet rule: reject if subnetRequestCount > 100
 * 6. If any rule triggers: return rejected (recorded in TRANSACTIONS with ACCEPTED=0)
 * 7. If accepted: update account balance
 */
public class ProcessTransaction extends VoltProcedure {

    // Phase 1: Validate account
    public final SQLStmt checkAccount = new SQLStmt(
        "SELECT ENABLED, CAST(BALANCE AS FLOAT), CAST(DAILY_LIMIT AS FLOAT) " +
        "FROM ACCOUNTS WHERE ACCOUNT_ID = ?;");

    // Phase 1: Validate merchant (replicated table)
    public final SQLStmt checkMerchant = new SQLStmt(
        "SELECT MERCHANT_ID FROM MERCHANTS WHERE MERCHANT_ID = ?;");

    // Phase 2: Insert transaction
    public final SQLStmt insertTxn = new SQLStmt(
        "INSERT INTO TRANSACTIONS (TXN_ID, ACCOUNT_ID, TXN_TIME, MERCHANT_ID, AMOUNT, " +
        "DEVICE_ID, SOURCE_IP, ACCEPTED, REASON, RULE_NAME) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    // Phase 3: Fraud rule checks via materialized views
    // Filter by the window that contains the current transaction's timestamp.
    // TIME_WINDOW(SECOND, 30, ?) computes which 30-second bucket the timestamp falls in,
    // matching exactly one row in the view — the window we just inserted into.
    // This avoids reading stale windows from prior bursts.
    public final SQLStmt checkTxn1Min = new SQLStmt(
        "SELECT TXN_COUNT, TOTAL_SPENT FROM TXN_SUMMARY_1MIN " +
        "WHERE ACCOUNT_ID = ? AND WINDOW_1MIN = TIME_WINDOW(SECOND, 60, ?);");

    public final SQLStmt checkTxn30Sec = new SQLStmt(
        "SELECT TXN_COUNT, TOTAL_SPENT FROM TXN_SUMMARY_30SEC " +
        "WHERE ACCOUNT_ID = ? AND WINDOW_30SEC = TIME_WINDOW(SECOND, 30, ?);");

    // Phase 4: Update balance on accepted transaction
    public final SQLStmt updateBalance = new SQLStmt(
        "UPDATE ACCOUNTS SET BALANCE = BALANCE + ? WHERE ACCOUNT_ID = ?;");

    private static final int MAX_SUBNET_REQUESTS = 100;
    private static final String ACCEPTED_REASON = "Accepted";

    private VoltTable buildResult(byte accepted, String reason, String ruleName) {
        VoltTable result = new VoltTable(
            new VoltTable.ColumnInfo("ACCEPTED", VoltType.TINYINT),
            new VoltTable.ColumnInfo("REASON", VoltType.STRING),
            new VoltTable.ColumnInfo("RULE_NAME", VoltType.STRING)
        );
        result.addRow(accepted, reason, ruleName);
        return result;
    }

    /**
     * @param accountId         partition key
     * @param txnId             unique transaction ID
     * @param txnTimeMs         transaction time in epoch milliseconds
     * @param merchantId        merchant identifier
     * @param amount            transaction amount
     * @param deviceId          device identifier
     * @param sourceIp          IP address of the requestor
     * @param subnetRequestCount current request count for the source IP's /24 subnet
     *                           (obtained from a prior RecordSubnetRequest call)
     */
    public VoltTable run(long accountId, long txnId, long txnTimeMs, int merchantId,
                         double amount, String deviceId, String sourceIp,
                         long subnetRequestCount) {

        TimestampType txnTime = new TimestampType(txnTimeMs * 1000);

        // ==========================================
        // Phase 1: Validate account and merchant
        // ==========================================
        voltQueueSQL(checkAccount, EXPECT_ZERO_OR_ONE_ROW, accountId);
        voltQueueSQL(checkMerchant, EXPECT_ZERO_OR_ONE_ROW, merchantId);
        VoltTable[] phase1 = voltExecuteSQL();

        VoltTable accountInfo = phase1[0];
        VoltTable merchantInfo = phase1[1];

        if (accountInfo.getRowCount() == 0) {
            String reason = "Invalid Account";
            voltQueueSQL(insertTxn, txnId, accountId, txnTime, merchantId, amount,
                         deviceId, sourceIp, (byte) 0, reason, "VALIDATION");
            voltExecuteSQL(true);
            return buildResult((byte) 0, reason, "VALIDATION");
        }
        if (merchantInfo.getRowCount() == 0) {
            String reason = "Invalid Merchant";
            voltQueueSQL(insertTxn, txnId, accountId, txnTime, merchantId, amount,
                         deviceId, sourceIp, (byte) 0, reason, "VALIDATION");
            voltExecuteSQL(true);
            return buildResult((byte) 0, reason, "VALIDATION");
        }

        accountInfo.advanceRow();
        byte enabled = (byte) accountInfo.getLong(0);

        if (enabled == 0) {
            String reason = "Account Disabled";
            voltQueueSQL(insertTxn, txnId, accountId, txnTime, merchantId, amount,
                         deviceId, sourceIp, (byte) 0, reason, "VALIDATION");
            voltExecuteSQL(true);
            return buildResult((byte) 0, reason, "VALIDATION");
        }

        // ==========================================
        // Phase 2: Insert the transaction (initially as accepted)
        // We insert before checking rules so the views include this TX.
        // ==========================================
        voltQueueSQL(insertTxn, txnId, accountId, txnTime, merchantId, amount,
                     deviceId, sourceIp, (byte) 1, ACCEPTED_REASON, "NONE");
        voltExecuteSQL();

        // ==========================================
        // Phase 3: Check threat detection rules
        // ==========================================
        String blockReason = null;
        String ruleName = null;

        // Rule: TXN_SUMMARY_1MIN — reject if >$5,000 spent in 1 minute
        voltQueueSQL(checkTxn1Min, accountId, txnTime);
        // Rule: TXN_SUMMARY_30SEC — reject if >5 txns in 30 seconds
        voltQueueSQL(checkTxn30Sec, accountId, txnTime);
        VoltTable[] fraudChecks = voltExecuteSQL();

        if (fraudChecks[0].advanceRow()) {
            BigDecimal totalSpent = fraudChecks[0].getDecimalAsBigDecimal(1);
            long spent = totalSpent != null ? totalSpent.longValue() : 0;
            if (spent > 5000) {
                blockReason = String.format(
                    "High Spending in 1 Minute (>$5,000): total $%d for account %d",
                    spent, accountId);
                ruleName = "TXN_SUMMARY_1MIN";
            }
        }

        if (blockReason == null && fraudChecks[1].advanceRow()) {
            long txnCount = fraudChecks[1].getLong(0);
            if (txnCount > 5) {
                blockReason = String.format(
                    "Too Many Transactions in 30 Seconds (>5): count %d for account %d",
                    txnCount, accountId);
                ruleName = "TXN_SUMMARY_30SEC";
            }
        }

        // Rule: SUBNET_RATE — reject if >100 requests from same /24 subnet
        if (blockReason == null && subnetRequestCount > MAX_SUBNET_REQUESTS) {
            blockReason = String.format(
                "Too Many Requests from Subnet (>%d): count %d from IP %s",
                MAX_SUBNET_REQUESTS, subnetRequestCount, sourceIp);
            ruleName = "SUBNET_RATE";
        }

        // ==========================================
        // Phase 4: Record result and commit
        // ==========================================
        if (blockReason != null) {
            // Blocked transaction is already recorded in TRANSACTIONS (ACCEPTED=0)
            voltExecuteSQL(true);
            return buildResult((byte) 0, blockReason, ruleName);
        }

        // Accepted — update balance
        voltQueueSQL(updateBalance, amount, accountId);
        voltExecuteSQL(true);
        return buildResult((byte) 1, ACCEPTED_REASON, "NONE");
    }
}