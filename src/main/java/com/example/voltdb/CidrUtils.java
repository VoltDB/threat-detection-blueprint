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

/**
 * Utility for CIDR subnet operations on IPv4 addresses.
 */
public final class CidrUtils {

    private CidrUtils() {}

    /**
     * Extract the network address from an IPv4 address using a CIDR prefix length.
     * Applies a bitmask to zero out host bits beyond the prefix.
     *
     * Examples:
     *   extractSubnet("192.168.1.45", 24) → "192.168.1.0"
     *   extractSubnet("192.168.1.45", 28) → "192.168.1.32"
     *   extractSubnet("10.50.50.99", 16) → "10.50.0.0"
     */
    public static String extractSubnet(String ip, int prefixLength) {
        String[] octets = ip.split("\\.");
        int addr = (Integer.parseInt(octets[0]) << 24)
                 | (Integer.parseInt(octets[1]) << 16)
                 | (Integer.parseInt(octets[2]) << 8)
                 |  Integer.parseInt(octets[3]);
        int mask = prefixLength == 0 ? 0 : 0xFFFFFFFF << (32 - prefixLength);
        int network = addr & mask;
        return ((network >> 24) & 0xFF) + "."
             + ((network >> 16) & 0xFF) + "."
             + ((network >>  8) & 0xFF) + "."
             + ( network        & 0xFF);
    }
}