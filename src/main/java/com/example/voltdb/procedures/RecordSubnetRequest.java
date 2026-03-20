package com.example.voltdb.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

/**
 * Single-partition procedure to record a request from a /24 CIDR subnet
 * and return the current request count for that subnet.
 *
 * Partitioned on SUBNET — all requests from the same /24 prefix are on the same partition.
 *
 * The returned count is passed to ProcessTransaction for the subnet rate rule check.
 * This separation allows subnet counting to happen outside the main transaction
 * while the rule evaluation remains inside the transaction.
 */
public class RecordSubnetRequest extends VoltProcedure {

    public final SQLStmt insertRequest = new SQLStmt(
        "INSERT INTO SUBNET_REQUESTS (REQUEST_ID, SUBNET, SOURCE_IP, REQUEST_TIME) " +
        "VALUES (?, ?, ?, ?);");

    public final SQLStmt getSubnetCount = new SQLStmt(
        "SELECT REQUEST_COUNT FROM REQUESTS_PER_SUBNET WHERE SUBNET = ?;");

    /**
     * @param subnet    the /24 subnet prefix (e.g., "192.168.1")
     * @param requestId unique request identifier (can be the transaction ID)
     * @param sourceIp  full IP address
     * @param requestTimeMs request time in epoch milliseconds
     * @return VoltTable with one row: SUBNET (string), REQUEST_COUNT (bigint)
     */
    public VoltTable run(String subnet, long requestId, String sourceIp, long requestTimeMs) {

        TimestampType requestTime = new TimestampType(requestTimeMs * 1000);

        // Insert the request record
        voltQueueSQL(insertRequest, requestId, subnet, sourceIp, requestTime);
        voltExecuteSQL();

        // Query the materialized view for current count
        voltQueueSQL(getSubnetCount, subnet);
        VoltTable[] results = voltExecuteSQL(true);

        long count = 0;
        if (results[0].advanceRow()) {
            count = results[0].getLong(0);
        }

        VoltTable result = new VoltTable(
            new VoltTable.ColumnInfo("SUBNET", VoltType.STRING),
            new VoltTable.ColumnInfo("REQUEST_COUNT", VoltType.BIGINT)
        );
        result.addRow(subnet, count);
        return result;
    }
}