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

import org.HdrHistogram.Histogram;

import benchmark.hbase.controller.BenchmarkType;

import com.google.common.base.Stopwatch;

public interface Report {

    public void printResults(final Histogram totalHistogram, long durationInSeconds, long totalUserRecords,
            long totalMatchRecords);

    public void aggregateAndPrintResults(final BenchmarkType benchMarkType,
            final CompletionService<Histogram> consumerCompletionService, final int numOfThreads,
            final long numOfRecords, final Stopwatch consumerStartTime);
}
