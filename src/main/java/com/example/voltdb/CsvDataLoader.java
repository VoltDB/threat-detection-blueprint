/* This file is part of VoltDB.
 * Copyright (C) 2026 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com.example.voltdb;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CsvDataLoader {

    /**
     * Load account data from CSV.
     * CSV columns: account_id,enabled,balance,daily_limit,name,email
     */
    public List<Long> loadAccountData(ThreatDetectionApp app, String resourcePath)
            throws Exception {
        List<Long> accountIds = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getResource(resourcePath)))) {
            String line = reader.readLine(); // skip header
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] fields = parseCsvLine(line);
                long accountId = Long.parseLong(fields[0].trim());
                byte enabled = Byte.parseByte(fields[1].trim());
                double balance = Double.parseDouble(fields[2].trim());
                double dailyLimit = Double.parseDouble(fields[3].trim());
                String name = fields[4].trim();
                String email = fields[5].trim();
                app.upsertAccount(accountId, enabled, balance, dailyLimit, name, email);
                if (app.getTransactionPublisher() != null) {
                    app.getTransactionPublisher().cacheAccount(accountId, name);
                }
                accountIds.add(accountId);
                System.out.printf("Loaded account: %d (%s)%n", accountId, name);
            }
        }
        System.out.printf("Total accounts loaded: %d%n", accountIds.size());
        return accountIds;
    }

    /**
     * Load merchant data from CSV.
     * CSV columns: merchant_id,name,category
     */
    public List<Integer> loadMerchantData(ThreatDetectionApp app, String resourcePath)
            throws Exception {
        List<Integer> merchantIds = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getResource(resourcePath)))) {
            String line = reader.readLine(); // skip header
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] fields = parseCsvLine(line);
                int merchantId = Integer.parseInt(fields[0].trim());
                String name = fields[1].trim();
                String category = fields[2].trim();
                app.upsertMerchant(merchantId, name, category);
                if (app.getTransactionPublisher() != null) {
                    app.getTransactionPublisher().cacheMerchant(merchantId, name, category);
                }
                merchantIds.add(merchantId);
                System.out.printf("Loaded merchant: %d (%s)%n", merchantId, name);
            }
        }
        System.out.printf("Total merchants loaded: %d%n", merchantIds.size());
        return merchantIds;
    }

    public String[] parseCsvLine(String line) {
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