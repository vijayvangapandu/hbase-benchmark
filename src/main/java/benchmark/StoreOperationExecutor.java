package benchmark;

import java.util.concurrent.Callable;

import org.HdrHistogram.Histogram;

public interface StoreOperationExecutor extends Callable<Histogram>, AutoCloseable {
}
