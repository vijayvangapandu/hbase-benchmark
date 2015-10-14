package benchmark.hbase.controller;

import benchmark.hbase.ReadAndWriteBenchMark;
import benchmark.hbase.ReadOnlyBenchMark;
import benchmark.hbase.WriteOnlyBenchMark;

public enum BenchmarkType {
    READ_ONLY(new ReadOnlyBenchMark()), WRITE_ONLY(new WriteOnlyBenchMark()), READ_AND_WRITE(
            new ReadAndWriteBenchMark());

    private final Benchmark benchmark;

    private BenchmarkType(final Benchmark benchmark) {
        this.benchmark = benchmark;
    }

    public Benchmark getBenchmark() {
        return benchmark;
    }
}