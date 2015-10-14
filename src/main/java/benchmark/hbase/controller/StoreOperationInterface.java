package benchmark.hbase.controller;

public interface StoreOperationInterface {

    public void benchMarkReadByKey(String key);
    public void benchMarkWrite(Object objectToWrite);
    
}
