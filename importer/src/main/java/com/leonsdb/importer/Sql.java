// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.util.Locale;

/** Checks on SQL text from a plan. */
final class Sql {

    private Sql() {
    }

    /**
     * Whether a query is one statement that only reads: SELECT, or WITH ...
     * SELECT, with no second statement after it. The source connection is
     * read-only and never committed as well; this refuses anything else
     * before it is even sent.
     *
     * @param query the query
     * @return true if it only reads
     */
    static boolean isSelect(String query) {
        String q = stripComments(query).trim();
        while (q.endsWith(";")) {
            q = q.substring(0, q.length() - 1).trim();
        }
        if (q.indexOf(';') >= 0 && semicolonOutsideQuotes(q)) {
            return false; // more than one statement
        }
        String upper = q.toUpperCase(Locale.ROOT);
        if (upper.startsWith("WITH")) {
            // A common table expression may wrap a write in some databases.
            for (String w : new String[] {"INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "DROP", "ALTER", "CREATE", "TRUNCATE"}) {
                if (upper.matches("(?s).*\\b" + w + "\\b.*")) {
                    return false;
                }
            }
            return true;
        }
        return upper.startsWith("SELECT") && !upper.matches("(?s).*\\bINTO\\s+(OUTFILE|DUMPFILE)\\b.*")
                && !upper.matches("(?s).*\\bFOR\\s+UPDATE\\b.*");
    }

    private static String stripComments(String s) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        char quote = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (quote != 0) {
                out.append(c);
                if (c == quote) {
                    quote = 0;
                }
                i++;
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                out.append(c);
                i++;
            } else if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-' || c == '#') {
                while (i < s.length() && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? s.length() : end + 2;
                out.append(' ');
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static boolean semicolonOutsideQuotes(String s) {
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
            } else if (c == ';') {
                return true;
            }
        }
        return false;
    }
}
