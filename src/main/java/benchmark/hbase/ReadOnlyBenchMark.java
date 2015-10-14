package benchmark.hbase;

import java.util.List;
import java.util.concurrent.CompletionService;

import org.HdrHistogram.Histogram;

import benchmark.hbase.controller.Benchmark;
import benchmark.hbase.report.Report;

import com.google.common.base.Stopwatch;

public class ReadOnlyBenchMark implements Benchmark {

    @Override
    public void run(List<StoreOperationCoordinator> storeOperationCoordinators, Report report) {
        StoreOperationCoordinator readCoordinator = storeOperationCoordinators.iterator().next();
        final CompletionService<Histogram> operationCompletionService = readCoordinator.invokeOperation();

        // Note that the timer is started after startConsumers(), this is by purpose to exclude the initialization time.
        final Stopwatch consumerStartTime = new Stopwatch().start();

        report.aggregateAndPrintResults(readCoordinator.getBenchMarkType(), operationCompletionService, 
                readCoordinator.getNumThreads(), readCoordinator.getNumRecords(), consumerStartTime);
  
    }

   
}
