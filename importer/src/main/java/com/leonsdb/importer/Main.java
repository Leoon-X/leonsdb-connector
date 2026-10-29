// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * The importer's command line:
 *
 * <pre>
 *   java -jar leonsdb-import.jar plan.yml                      dry run: checks, writes nothing
 *   java -jar leonsdb-import.jar plan.yml --apply              imports
 *   java -jar leonsdb-import.jar plan.yml --apply --allow-skips
 * </pre>
 *
 * The report is printed and also written next to the plan (plan-report.txt),
 * or to the file given with --report.
 */
public final class Main {

    private Main() {
    }

    /**
     * @param args the command line
     */
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, System.getenv(), Drivers.standard(System.out)));
    }

    static int run(String[] args, PrintStream out, PrintStream err, Map<String, String> env, Drivers drivers) {
        Path planFile = null;
        Path reportFile = null;
        boolean apply = false;
        boolean allowSkips = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--apply" -> apply = true;
                case "--allow-skips" -> allowSkips = true;
                case "--report" -> {
                    if (++i == args.length) {
                        return usage(err, "--report needs a file name");
                    }
                    reportFile = Path.of(args[i]);
                }
                case "-h", "--help" -> {
                    usage(out, null);
                    return 0;
                }
                default -> {
                    if (args[i].startsWith("-") || planFile != null) {
                        return usage(err, "unknown argument " + args[i]);
                    }
                    planFile = Path.of(args[i]);
                }
            }
        }
        if (planFile == null) {
            return usage(err, "name the plan file");
        }
        if (allowSkips && !apply) {
            return usage(err, "--allow-skips only goes with --apply");
        }
        Plan plan;
        try {
            plan = Plan.load(planFile, env);
        } catch (ImportException e) {
            err.println("The plan is not valid: " + e.getMessage());
            return 1;
        }
        Importer.Outcome outcome;
        try (LeonsDBTarget target = new LeonsDBTarget(plan.leonsdb())) {
            outcome = new Importer(plan, target, Importer.sources(drivers), out).run(apply, allowSkips);
        }
        out.println();
        out.print(outcome.report());
        Path report = reportFile != null ? reportFile : reportNextTo(planFile);
        try {
            Files.writeString(report, outcome.report(), StandardCharsets.UTF_8);
            out.println("The report is in " + report);
        } catch (IOException e) {
            err.println("Could not write the report to " + report + ": " + e.getMessage());
        }
        return outcome.ok() ? 0 : 1;
    }

    private static Path reportNextTo(Path plan) {
        String name = plan.getFileName().toString();
        String base = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
        Path dir = plan.toAbsolutePath().getParent();
        return dir.resolve(base + "-report.txt");
    }

    private static int usage(PrintStream to, String problem) {
        if (problem != null) {
            to.println(problem);
        }
        to.println("Usage: java -jar leonsdb-import.jar <plan.yml> [--apply [--allow-skips]] [--report <file>]");
        to.println("  Without --apply, a dry run: everything is checked and nothing is written.");
        to.println("  See docs/importing.md for how to write a plan.");
        return 2;
    }
}
