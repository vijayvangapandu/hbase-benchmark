package benchmark.hbase;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import benchmark.StoreOperationExecutor;
import benchmark.hbase.data.WriteWorkRecordGenerator;
import benchmark.hbase.report.Histograms;
import benchmark.hbase.util.PhoenixConnectionManager;

public class HBaseWriteOperationExecutor implements StoreOperationExecutor {

    private static final Logger log = LoggerFactory.getLogger(HBaseWriteOperationExecutor.class);
    private final WriteWorkRecordGenerator workRecordGenerator;
    private final Histogram histogram;

    public HBaseWriteOperationExecutor(final WriteWorkRecordGenerator workRecordGenerator) {
        this.workRecordGenerator         = Preconditions.checkNotNull(workRecordGenerator);
        this.histogram      =  Histograms.create();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public Histogram call() throws Exception {
        int messageCount = 0;

        try {
            String writeQuery = workRecordGenerator.generateNextWorkRecord();
            int updateStatus = 0;
            Connection conn =  PhoenixConnectionManager.getConnection();
            while(StringUtils.isNotEmpty(writeQuery)) {
                final Stopwatch stopwatch = new Stopwatch().start();
                updateStatus = conn.prepareStatement(writeQuery).executeUpdate();
                if(updateStatus ==0) {
                    log.info("update unsuccessfull...");
                } else {
                    log.debug("update successfull...");
                }
                messageCount++;
                stopwatch.stop();
                histogram.recordValue(stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
                writeQuery = workRecordGenerator.generateNextWorkRecord();
            }
            
        } catch (final Exception e) {
            log.info("exception whil reading records from hbase...", e);
        }

        log.info("In total  {} messages saved to store", messageCount);

        return histogram;
    }

}
