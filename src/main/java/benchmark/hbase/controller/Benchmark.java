package benchmark.hbase.controller;

import java.util.List;

import benchmark.hbase.StoreOperationCoordinator;
import benchmark.hbase.report.Report;

public interface Benchmark {

    public void run(final List<StoreOperationCoordinator> storeOperationCoordinators, final Report report);
}
