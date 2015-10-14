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

package benchmark.hbase;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.HdrHistogram.Histogram;

import benchmark.StoreOperationExecutor;
import benchmark.hbase.controller.BenchmarkType;

/**
 * Defines common code for coordinating message producers.
 * Needs to be overridden to add specific implementations for message producer creation.
 */
public abstract class StoreOperationCoordinator implements AutoCloseable {
    private final int numThreads;
    private final long numRecords;
    private final BenchmarkType benchMarkType;

    public int getNumThreads() {
        return numThreads;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public BenchmarkType getBenchMarkType() {
        return benchMarkType;
    }

    private final CompletionService<Histogram> executorCompletionService;

    public StoreOperationCoordinator(final long numRecords, final int numThreads, final BenchmarkType benchMarkType) {
        this.numThreads                     = numThreads;
        this.numRecords                     = numRecords;
        this.benchMarkType                  = benchMarkType;
        this.executorCompletionService      = new ExecutorCompletionService<Histogram>(Executors.newFixedThreadPool(numThreads));
    }

    public abstract CompletionService<Histogram> invokeOperation();
    public abstract StoreOperationExecutor createStoreOperationExecutor(long numRecords);
}