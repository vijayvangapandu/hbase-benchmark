package benchmark.hbase;

public class WorkRecordGenerator {

    private final long totalRrecordsCount;
    private int startRecordKey;
    private long processedRecordCount;
    
    public WorkRecordGenerator(final long totalRrecordsCount) {
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
    
    public String generateNextReadWorkRecord() {
        if(processedRecordCount++ < totalRrecordsCount) {
            return "select * from matches_feed where uid =1";
        } else {
            return null;
        }
    }
    
    public String generateNextWriteWorkRecord() {
        if(processedRecordCount++ < totalRrecordsCount) {
            return "upsert into matches_feed (uid, mid, muid, fname) values (1,12,2,'test1')";
        } else {
            return null;
        }
    }
    
    
}
