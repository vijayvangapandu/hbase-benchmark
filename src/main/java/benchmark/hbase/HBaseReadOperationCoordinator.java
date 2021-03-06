package benchmark.hbase;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.HdrHistogram.Histogram;

import benchmark.StoreOperationExecutor;
import benchmark.hbase.controller.BenchmarkType;
import benchmark.hbase.data.ReadWorkRecordGenerator;

public class HBaseReadOperationCoordinator extends StoreOperationCoordinator {

    private final int numThreads;
    private final long numRecords;

    private final CompletionService<Histogram> executorCompletionService;

    public HBaseReadOperationCoordinator(final long numRecords, final int numThreads) {
        super(numRecords, numThreads, BenchmarkType.READ_ONLY);

        this.numThreads                     = numThreads;
        this.numRecords                     = numRecords;
        this.executorCompletionService      = new ExecutorCompletionService<Histogram>(Executors.newFixedThreadPool(numThreads));
    }

    public CompletionService<Histogram> invokeOperation() {
        final long numberOfRecordsPerThread = numRecords / numThreads;

        for (int i = 0; i < numThreads; i++) {
            final StoreOperationExecutor storeOperationExecutor = createStoreOperationExecutor(numberOfRecordsPerThread);
            executorCompletionService.submit(storeOperationExecutor);
        }

        return executorCompletionService;
    }

    @Override
    public StoreOperationExecutor createStoreOperationExecutor(long numRecords) {
        ReadWorkRecordGenerator workRecordGenerator =  new ReadWorkRecordGenerator(numRecords);
        final StoreOperationExecutor storeOperationExecutor = new HBaseReadOperationExecutor(workRecordGenerator);
        return storeOperationExecutor;
    }

    @Override
    public void close() throws Exception {
        
    }
}
