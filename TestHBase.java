import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.util.*; 
import org.apache.hadoop.hbase.util.Bytes; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestHBase { 
    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }
    public static void main(String[] args) throws Exception { 
        //Config Settings 
        //Configuration conf = HBaseConfiguration.create(); 
        HBaseAdmin admin = new HBaseAdmin(conf); 

        try{ 

            //Create Table ===============================
            String table_a ="table1_Raul";
            String table_b ="table2_Raul";
            if (admin.tableExists(table_a)) {
                System.out.println("table already exists!");
            } else {
                HTableDescriptor descriptor = new HTableDescriptor(table_a); 
                descriptor.addFamily(new HColumnDescriptor("City")); 
                descriptor.addFamily(new HColumnDescriptor("State")); 

                admin.createTable(descriptor);
                System.out.println("create table " + table_a + " ok.");
                // Fill Tables=============================== ====
                addRecord(table_a,"row1","City", "", "Jacksonville");
                addRecord(table_a,"row2","City", "", "Miami");
                addRecord(table_a,"row3","City", "", "Nashville");

                addRecord(table_a,"row1","State", "", "FL");
                addRecord(table_a,"row2","State", "", "FL");
                addRecord(table_a,"row3","State", "", "TN");
            }
            if (admin.tableExists(table_b)) {
                System.out.println("table already exists!");
            } else {
                HTableDescriptor descriptor = new HTableDescriptor(table_b); 
                descriptor.addFamily(new HColumnDescriptor("City")); 
                descriptor.addFamily(new HColumnDescriptor("Area")); 

                admin.createTable(descriptor);
                System.out.println("create table " + table_b + " ok.");
                addRecord(table_b,"row1","City", "", "Jacksonville");
                addRecord(table_b,"row2","City", "", "Miami");
                addRecord(table_b,"row3","City", "", "New Orleands");

                addRecord(table_b,"row1","Area", "", "South");
                addRecord(table_b,"row2","Area", "", "South");
                addRecord(table_b,"row3","Area", "", "South");
            }
            joinTables( table_a, table_b, "City", "", "City", "");
        } finally { 
            admin.close(); 
        } 
    } 
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void joinTables(String table1name, String table2name, String family1, String qualifier1,String family2, String qualifier2) throws Exception {
        try {
             HTable table1 = new HTable(conf, table1name);
             HTable table2 = new HTable(conf, table2name);
             Scan s = new Scan();
             ResultScanner ss1 = table1.getScanner(s);
             int i = 1;
             for(Result r1:ss1){
                 for(KeyValue kv1 : r1.raw()){
                    if(new String(kv1.getFamily()).equals( family1)){
                        ResultScanner ss2 = table2.getScanner(s);
                        for(Result r2:ss2){
                            for(KeyValue kv2 : r2.raw()){
                                if(new String(kv2.getFamily()).equals( family2)){

                                    if (new String(kv1.getValue()).equals(new String(kv2.getValue()))){
                                        String newRow = "row" + i;
                                        i++;
                                        System.out.print(newRow + " ");
                                        Get get = new Get(kv2.getRow());
                                        Result row = table2.get(get);
                                        //System.out.print(new String(kv1.getRow()) + " ");
                                        System.out.print(new String(kv1.getFamily()) + ":");
                                        System.out.print(new String(kv1.getQualifier()) + " ");
                                        System.out.print(new String(kv1.getValue())+" ");
                                        for(KeyValue kv3 : row.raw()){
                                             if(!(new String(kv3.getFamily()).equals( family2))){
                                                //System.out.print(new String(kv.getRow()) + " " );
                                                System.out.print(new String(kv3.getFamily()) + ":" );
                                                System.out.print(new String(kv3.getQualifier()) + " " );
                                                //System.out.print(kv.getTimestamp() + " " );
                                                System.out.println(new String(kv3.getValue()));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                 }
             }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void getAllRecord (String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r:ss){
                 for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                 }
             }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
}