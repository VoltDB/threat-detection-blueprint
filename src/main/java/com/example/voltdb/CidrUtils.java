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