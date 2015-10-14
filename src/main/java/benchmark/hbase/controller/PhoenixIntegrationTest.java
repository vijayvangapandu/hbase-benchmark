package benchmark.hbase.controller;

import java.sql.Connection;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import benchmark.hbase.util.PhoenixConnectionManager;

public class PhoenixIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(PhoenixIntegrationTest.class);

    private final String insertQueryPart1;
    private static final int numberOfUsers = 100000;
    private static final int startUserId = 27000;
    private static final int startCandidateId = 300000;
    private static final int numberOfMatchesForUser = 5000;
    private static int matchIdStart = 10070000;

    //24K - 5 matches
    //26K - 10 matches
    //26K - 10 matches
    public PhoenixIntegrationTest() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder
                .append("UPSERT INTO matches_feed(uid,mid,muid, one_way, lmod, mstate, cstate, astate, isu, delivdt, closedt, relaxed, distance");
        queryBuilder
                .append(", initlzr, comStage, lcomdt, comstdt, prreaddt, cmhcsdt, ibstat, ftr, tw, fname, bd, gid, city, state, country, hphoto, cscore)");
        queryBuilder.append("VALUES(");
        insertQueryPart1 = queryBuilder.toString();
    }

    public static void main(String args[]) throws Exception {
        PhoenixIntegrationTest test = new PhoenixIntegrationTest();
        test.insertDataInfoMatchesFeed();

    }

    private void insertDataInfoMatchesFeed() throws Exception {
          Connection conn;
          conn =  PhoenixConnectionManager.getConnection();
          
          /*System.out.println("got connection");
          long totalStartTime = System.currentTimeMillis();
          for(int userId=startUserId; userId < startUserId + numberOfUsers; userId++) {
              long startTime = System.currentTimeMillis();
              for(int candiadteUserId=startCandidateId; candiadteUserId < startCandidateId + numberOfMatchesForUser; candiadteUserId++) {
                  StringBuilder queryBuilder2 = new StringBuilder(insertQueryPart1);
                  queryBuilder2.append(String.valueOf(userId)).append(", ");
                  queryBuilder2.append(String.valueOf(matchIdStart++)).append(", ");
                  queryBuilder2.append(String.valueOf(candiadteUserId)).append(", ");
                  queryBuilder2.append("0,null,0,0,0,TRUE,'1980-12-20 2:35:34',null,0,10,1,0,null,null,null,null,0,0,0,'vijay','2015-12-20 2:35:34',1,'Los Angeles','CA',1,TRUE,200)");
                  conn.createStatement().executeUpdate(queryBuilder2.toString());
              }
              conn.commit();
              long endTime = System.currentTimeMillis();
              System.out.println("Committed results for user:"+ userId +" in duration :" +  (endTime - startTime));
          }
          long totalEndTime = System.currentTimeMillis();
          System.out.println("Total insert time duration" +  (totalEndTime - totalStartTime));*/
          
          long totalStartTime = System.currentTimeMillis();
          
          for(int userId=startUserId; userId < startUserId + numberOfUsers; userId++ ) {
              int totalResults = 0;
              long startTime = System.currentTimeMillis();
              ResultSet rst = conn.createStatement().executeQuery("select * from matches_feed where uid = "+userId);
              while (rst.next()) {
                //System.out.println(rst.getString(1) + " " + rst.getString(2));
                  totalResults++;
              }
              long endTime = System.currentTimeMillis();
              System.out.println("Retrieved results for user:"+ userId +" in duration :" +  (endTime - startTime));
              System.out.println("Total Results Fetched For User:"+userId +" is "+totalResults);
          }
          long totalEndTime = System.currentTimeMillis();
          System.out.println("Total fetch duration" +  (totalEndTime - totalStartTime));
          
          conn.commit();
      }
}
