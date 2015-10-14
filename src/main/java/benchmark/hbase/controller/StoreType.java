package benchmark.hbase.controller;

public enum StoreType {
    HBASE(1, "HBASE");
    
    final String name;
    final int code;
    
    private StoreType(int code, String name) {
        this.name= name;
        this.code = code;
    }
    
    public static StoreType fromName(String name) {
        if("HBASE".equalsIgnoreCase(name)) {
            return HBASE;
        }
        
        return null;
    }
}
