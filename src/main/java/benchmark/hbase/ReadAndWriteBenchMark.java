package benchmark.hbase;

import java.util.List;
import java.util.concurrent.CompletionService;

import org.HdrHistogram.Histogram;

import com.google.common.base.Stopwatch;

import benchmark.hbase.controller.Benchmark;
import benchmark.hbase.report.Report;

public class ReadAndWriteBenchMark implements Benchmark {

    @Override
    public void run(List<StoreOperationCoordinator> storeOperationCoordinators, Report report) {
        for(StoreOperationCoordinator storeOperationCoordinator : storeOperationCoordinators) {
            final CompletionService<Histogram> operationCompletionService = storeOperationCoordinator.invokeOperation();

            // Note that the timer is started after startConsumers(), this is by purpose to exclude the initialization time.
            final Stopwatch consumerStartTime = new Stopwatch().start();

            report.aggregateAndPrintResults(storeOperationCoordinator.getBenchMarkType(), operationCompletionService, 
                    storeOperationCoordinator.getNumThreads(), storeOperationCoordinator.getNumRecords(), consumerStartTime);
        }
    }

   
}
