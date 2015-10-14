package benchmark.hbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import benchmark.StoreOperationExecutor;
import benchmark.hbase.data.ReadWorkRecordGenerator;
import benchmark.hbase.report.Histograms;
import benchmark.hbase.util.PhoenixConnectionManager;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

public class HBaseReadOperationExecutor implements StoreOperationExecutor {

    private static final Logger log = LoggerFactory.getLogger(HBaseReadOperationExecutor.class);
    private final ReadWorkRecordGenerator workRecordGenerator;
    private final Histogram histogram;

    public HBaseReadOperationExecutor(final ReadWorkRecordGenerator workRecordGenerator) {
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
            String readQuery = workRecordGenerator.generateNextWorkRecord();
            ResultSet rs= null;
            Connection conn =  PhoenixConnectionManager.getConnection();
            while(StringUtils.isNotEmpty(readQuery)) {
                final Stopwatch stopwatch = new Stopwatch().start();
                rs = conn.prepareStatement(readQuery).executeQuery();
                if(rs != null && rs.next()) {
                    Object uid = rs.getObject("uid");
                    if(uid != null) {
                        log.debug("select success for user {}", uid);
                    }
                } 
                messageCount++;
                stopwatch.stop();
                histogram.recordValue(stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
                readQuery = workRecordGenerator.generateNextWorkRecord();
            }
            
        } catch (final Exception e) {
            log.info("exception whil reading records from hbase...", e);
        }

        log.info("In total  {} messages retrieved from store", messageCount);

        return histogram;
    }
}
