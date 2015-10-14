/*
 * Copyright 2015 eHarmony, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark.hbase.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import benchmark.hbase.HBaseReadOperationCoordinator;
import benchmark.hbase.HBaseWriteOperationCoordinator;
import benchmark.hbase.StoreOperationCoordinator;
import benchmark.hbase.report.LoggingReport;
import benchmark.hbase.report.Report;

import com.google.common.base.Optional;

public class TestLauncher {
    private final static Logger log = LoggerFactory.getLogger(TestLauncher.class);

    private static final Options options = initializeOptions();

    public static void start(final StoreType storeType, final BenchmarkType benchmarkType, final long numReads,
            final long numWrites, final int concurrancySize, final String connectionUrl) throws Exception {

        List<StoreOperationCoordinator> storeOperationCoordinators = new ArrayList<StoreOperationCoordinator>();

        final Report report = new LoggingReport();
        switch (benchmarkType) {
        case READ_ONLY:
            storeOperationCoordinators.add(new HBaseReadOperationCoordinator(numReads, concurrancySize));
        case WRITE_ONLY:
            storeOperationCoordinators.add(new HBaseWriteOperationCoordinator(numWrites, concurrancySize));
        case READ_AND_WRITE:
            storeOperationCoordinators.add(new HBaseWriteOperationCoordinator(numWrites, concurrancySize));
            storeOperationCoordinators.add(new HBaseReadOperationCoordinator(numReads, concurrancySize));
        }
        benchmarkType.getBenchmark().run(storeOperationCoordinators, report);

    }

    @SuppressWarnings("static-access")
    private static Options initializeOptions() {
        final Options options = new Options();

        options.addOption("u", "usage", false, "show usage");

        // Required options
        options.addOption(OptionBuilder.withLongOpt("store-type")
                .withDescription("store type => HBASE | CASSANDRA | MONGODB").isRequired().hasArg().create());
        options.addOption(OptionBuilder.withLongOpt("num-reads").withDescription("number of reads")
                .hasArg().create());
        options.addOption(OptionBuilder.withLongOpt("num-writes").withDescription("number of writes")
                .hasArg().create());
        options.addOption(OptionBuilder.withLongOpt("read-concurrancy").withDescription("Read concurrancy")
                .isRequired().hasArg().create());

        options.addOption(OptionBuilder.withLongOpt("write-concurrancy").withDescription("Write concurrancy")
                .isRequired().hasArg().create());

        // Optional options
        options.addOption(OptionBuilder
                .withLongOpt("benchmark-type")
                .withDescription(
                        "benchmark type, one of READ_ONLY | WRITE_ONLY | READ_AND_WRITE. Will default to READ_ONLY, if not specified.")
                .hasArg().create());

        options.addOption(OptionBuilder.withLongOpt("connection-url").withDescription("connection url").hasArg()
                .create());

        return options;
    }

    /**
     * Displays help for the command line arguments.
     */
    private static void displayHelp() {
        // This prints out some help
        final HelpFormatter formater = new HelpFormatter();

        formater.printHelp(TestLauncher.class.getName(), options);
        System.exit(0);
    }

    public static void main(final String[] args) throws Exception {
        // create the parser
        final CommandLineParser parser = new BasicParser();

        // parse the command line arguments
        final CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("u")) {
            displayHelp();
        }

        final String connectionUrl = cmd.getOptionValue("connection-url");
        final StoreType storeType = StoreType.fromName(cmd.getOptionValue("store-type"));

        final Optional<String> optionalNumReads = Optional.fromNullable(cmd.getOptionValue("num-reads"));
        final Optional<String> optionalNumWrites = Optional.fromNullable(cmd.getOptionValue("num-writes"));
        final int readConcurrancy = Integer.parseInt(cmd.getOptionValue("read-concurrancy"));
        final int writeConcurrancy = Integer.parseInt(cmd.getOptionValue("write-concurrancy"));

        int numReads = 0;
        int numWrites = 0;

        BenchmarkType benchmarkType = BenchmarkType.READ_ONLY;
        if (optionalNumReads.isPresent() && optionalNumWrites.isPresent()) {
            benchmarkType = BenchmarkType.READ_AND_WRITE;
            numReads = Integer.parseInt(optionalNumReads.get());
            numWrites = Integer.parseInt(optionalNumWrites.get());
        } else if (optionalNumReads.isPresent()) {
            benchmarkType = BenchmarkType.READ_ONLY;
            numReads = Integer.parseInt(optionalNumReads.get());
        } else if (optionalNumWrites.isPresent()) {
            benchmarkType = BenchmarkType.WRITE_ONLY;
            numWrites = Integer.parseInt(optionalNumWrites.get());
        }

        log.info("connectionUrl: {}", connectionUrl);
        log.info("storeType: {}", storeType);
        log.info("numReads: {}", numReads);
        log.info("numWrites: {}", numWrites);
        log.info("readConcurrancy: {}", readConcurrancy);
        log.info("writeConcurrancy: {}", writeConcurrancy);
        log.info("benchmarkType: {}", benchmarkType);

        TestLauncher.start(storeType, benchmarkType, numReads, numWrites, readConcurrancy, connectionUrl);

        System.exit(0);
    }
}
