package benchmark.hbase.data;

public class WriteWorkRecordGenerator implements WorkRecordGenerator {

    private final long totalRrecordsCount;
    private int startRecordKey;
    private long processedRecordCount;

    public WriteWorkRecordGenerator(final long totalRrecordsCount) {
        this.totalRrecordsCount = totalRrecordsCount;
    }

    public long getTotalRecordsCount() {
        return totalRrecordsCount;
    }

    public int getStartRecordKey() {
        return startRecordKey;
    }

    public void setStartRecordKey(int startRecordKey) {
        this.startRecordKey = startRecordKey;
    }

    @Override
    public String generateNextWorkRecord() {
        if (processedRecordCount++ < totalRrecordsCount) {
            // return "select * from matches_feed where uid =1 and mid=12";
            return "select * from matches_feed where uid =27000";
        } else {
            return null;
        }
    }

}
