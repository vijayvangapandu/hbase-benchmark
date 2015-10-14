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

package benchmark.hbase.report;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import benchmark.hbase.controller.BenchmarkType;

import com.google.common.base.Stopwatch;

public class LoggingReport implements Report {

    private final static Logger log = LoggerFactory.getLogger(LoggingReport.class);

    @Override
    public void printResults(final Histogram totalHistogram, long durationInSeconds, long totalUserRecords, long totalMatchRecords) {

        final long durationInMs = durationInSeconds * 1000;
        final long usersThroughputPerSecond = 1000 * totalUserRecords / durationInMs;
        final long matchesThroughputPerSecond = 1000 * totalMatchRecords / durationInMs;

        final long min = totalHistogram.getMinValue();
        final double percentile25 = totalHistogram.getValueAtPercentile(25);
        final double percentile50 = totalHistogram.getValueAtPercentile(50);
        final double percentile75 = totalHistogram.getValueAtPercentile(75);
        final double percentile95 = totalHistogram.getValueAtPercentile(95);
        final double percentile99 = totalHistogram.getValueAtPercentile(99);
        
        final long max = totalHistogram.getMaxValue();
        final double mean = totalHistogram.getMean();
        final double stdDev = totalHistogram.getStdDeviation();

        logInfo("=======================================");
        logInfo("HBase Benchmark stats");
        logInfo("=======================================");
        logInfo("DURATION (SECOND):      {}", durationInSeconds);
        logInfo("USERS THROUGHPUT / SECOND:    {}", usersThroughputPerSecond);
        logInfo("MATCHES THROUGHPUT / SECOND:    {}", matchesThroughputPerSecond);
        logInfo("MIN:                    {}", min);
        logInfo("25th percentile:        {}", percentile25);
        logInfo("50th percentile:        {}", percentile50);
        logInfo("75th percentile:        {}", percentile75);
        logInfo("95th percentile:        {}", percentile95);
        logInfo("99th percentile:        {}", percentile99);
        logInfo("MAX:                    {}", max);
        logInfo("MEAN:                   {}", mean);
        logInfo("STD DEVIATION:          {}", stdDev);
        logInfo("\n\n\n");
    }

    private void logInfo(String part) {
        logInfo(part, null);
    }

    private void logInfo(String part, double value) {
        logInfo(part, String.valueOf(value));
    }

    private void logInfo(String part, long value) {
        logInfo(part, String.valueOf(value));
    }

    private void logInfo(String part, String value) {
        if (StringUtils.isNotBlank(value)) {
            log.info(part, value);
            //System.out.println(part + " : " + value);
        } else {
            log.info(part);
            //System.out.println(part);
        }

    }

    @Override
    public void aggregateAndPrintResults(BenchmarkType benchMarkType,
            CompletionService<Histogram> executorCompletionService, int numOfThreads, long numOfRecords,
            Stopwatch executorStartTime) {
        
     // Used to accumulate results from all histograms.
        final Histogram totalHistogram = Histograms.create();

        for (int i = 0; i < numOfThreads; i++) {
            try {
                final Future<Histogram> histogramFuture = executorCompletionService.take();
                Histogram histogram = histogramFuture.get();
                totalHistogram.add(histogram);
            } catch (final InterruptedException e) {
                log.error("Failed to retrieve data, got inturrupt signal", e);
                Thread.currentThread().interrupt();

                break;
            } catch (final ExecutionException e) {
                log.error("Failed to retrieve data", e);
            }
        }

        executorStartTime.stop();
        final long durationInSeconds    = executorStartTime.elapsedTime(TimeUnit.SECONDS);
        final long durationInMs         = executorStartTime.elapsedTime(TimeUnit.MILLISECONDS);
        // Using the durationInMs, since I would loose precision when using durationInSeconds.
        final long throughputPerSecond  = 1000 * numOfRecords / durationInMs;

        final long min              = totalHistogram.getMinValue();
        final double percentile25   = totalHistogram.getValueAtPercentile(25);
        final double percentile50   = totalHistogram.getValueAtPercentile(50);
        final double percentile75   = totalHistogram.getValueAtPercentile(75);
        final double percentile95   = totalHistogram.getValueAtPercentile(95);
        final double percentile99   = totalHistogram.getValueAtPercentile(99);
        final long max              = totalHistogram.getMaxValue();
        final double mean           = totalHistogram.getMean();
        final double stdDev         = totalHistogram.getStdDeviation();
        final long totalMessagesCount = totalHistogram.getTotalCount();

        logInfo("=======================================");
        if (benchMarkType == BenchmarkType.READ_ONLY) {
            logInfo("READ ONLY BENCHMARK STATS");
        } else if(benchMarkType == BenchmarkType.WRITE_ONLY) {
            logInfo("WRITE ONLY BENCHMARK STATS");
        } else if(benchMarkType == BenchmarkType.READ_AND_WRITE) {
            logInfo("READ AND WRITE BENCHMARK STATS");
        } else {
            logInfo("UNKNOWN BENCHMARK STATS");
        }
        logInfo("=======================================");
        logInfo("DURATION (SECOND):      {}", durationInSeconds);
        logInfo("THROUGHPUT / SECOND:    {}", throughputPerSecond);
        logInfo("MIN:                    {}", min);
        logInfo("25th percentile:        {}", percentile25);
        logInfo("50th percentile:        {}", percentile50);
        logInfo("75th percentile:        {}", percentile75);
        logInfo("95th percentile:        {}", percentile95);
        logInfo("99th percentile:        {}", percentile99);
        logInfo("MAX:                    {}", max);
        logInfo("MEAN:                   {}", mean);
        logInfo("STD DEVIATION:          {}", stdDev);
        logInfo("CONCURRANCY:            {}", numOfThreads);
        logInfo("TotalRecords:           {}", totalMessagesCount);
        logInfo("\n\n\n");
        
    }

}
