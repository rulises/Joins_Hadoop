package hbase_mapred;


import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.*; 
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.util.*; 
import org.apache.hadoop.hbase.util.Bytes; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import hbase_mapred.RowC;
public class Join { 
	//CONFIGURATION
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
	}

	//MAPPER
	public static class MapperA extends TableMapper <Text,Put>{
		public static final byte[] CF = "attr".getBytes();
		public static final byte[] CSUB = "subject".getBytes();
		public static final byte[] CPRE = "predicate".getBytes();
		public static final byte[] COBJ = "object".getBytes();
		public static final byte[] CTAG = "tag".getBytes();
		public static final byte[] ATTR1 = "".getBytes();
		private final IntWritable ONE = new IntWritable(1);
   		private Text key = new Text();
		//@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			String val = new String(value.getValue(CF, ATTR1));

          			key.set(val);
          			Put p= new Put("row".getBytes());
          			p.add(CSUB, Bytes.toBytes(""), value.getValue(CSUB, ATTR1)); 
          			p.add(CPRE, Bytes.toBytes(""),value.getValue(CPRE, ATTR1)); 
          			p.add(COBJ, Bytes.toBytes(""), value.getValue(COBJ, ATTR1));
          			p.add(CTAG, Bytes.toBytes(""), value.getValue(CTAG, ATTR1));

			context.write(key, p);
		}
	}
	//Sel-Join Improvement
	public static class MapperC extends TableMapper <Text,Put>{
		public static final byte[] CF = "attr".getBytes();
		public static final byte[] CSUB = "subject".getBytes();
		public static final byte[] CPRE = "predicate".getBytes();
		public static final byte[] COBJ = "object".getBytes();
		public static final byte[] CTAG = "tag".getBytes();
		public static final byte[] ATTR1 = "".getBytes();
		private final IntWritable ONE = new IntWritable(1);
   		private Text key = new Text();
		//@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
			
			//ATTR A
			String val = new String(value.getValue(CSUB, ATTR1));
          			key.set(val);
          			Put p= new Put("row".getBytes());
          			p.add(CSUB, Bytes.toBytes(""), value.getValue(CSUB, ATTR1)); 
          			p.add(CPRE, Bytes.toBytes(""),value.getValue(CPRE, ATTR1)); 
          			p.add(COBJ, Bytes.toBytes(""), value.getValue(COBJ, ATTR1));
          			p.add(CTAG, Bytes.toBytes(""), Bytes.toBytes("A"));

			context.write(key, p);
			//ATTR B
			val = new String(value.getValue(CPRE, ATTR1));
          			key.set(val);
          			Put p2= new Put("row".getBytes());
          			p2.add(CSUB, Bytes.toBytes(""), value.getValue(CSUB, ATTR1)); 
          			p2.add(CPRE, Bytes.toBytes(""),value.getValue(CPRE, ATTR1)); 
          			p2.add(COBJ, Bytes.toBytes(""), value.getValue(COBJ, ATTR1));
          			p2.add(CTAG, Bytes.toBytes(""), Bytes.toBytes("B"));
			context.write(key, p2);


		}
	}
	
	public static class ReducerA extends TableReducer<Text, Put, ImmutableBytesWritable>  {
		public static final byte[] CF = "attr".getBytes();
		public static final byte[] CSUB = "subject".getBytes();
		public static final byte[] CPRE = "predicate".getBytes();
		public static final byte[] COBJ = "object".getBytes();
		public static final byte[] CTAG = "tag".getBytes();
		public static final byte[] ATTR1 = "".getBytes();
		public static int COUNT =0;
		//@Override
 		public void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException  {	
 			COUNT++;
			ArrayList<Row> batch = new ArrayList<Row>();

    			for (Put val_out : values) {
    					List<KeyValue> ls = val_out.get(CSUB,Bytes.toBytes(""));
    					List<KeyValue> lp = val_out.get(CPRE,Bytes.toBytes(""));
    					List<KeyValue> lo = val_out.get(COBJ,Bytes.toBytes(""));
    					List<KeyValue> lt = val_out.get(CTAG,Bytes.toBytes(""));

    					KeyValue kvs = ls.get( 0 ); 
    					KeyValue kvp=lp.get( 0 ); 
    					KeyValue kvo=lo.get( 0 ); 
    					KeyValue kvt=lt.get( 0 ); 
	    					

    					byte[] s1 = kvs.getValue();
    					byte[] p1 = kvp.getValue();
    					byte[] o1 = kvo.getValue();
    					byte[] t1 = kvt.getValue();
    					for (Put val_in : values) {
    						List<KeyValue> lt2 = val_in.get(CTAG,Bytes.toBytes(""));
	    					KeyValue kvt2=lt2.get( 0 ); 
	    					byte[] t2 = kvt2.getValue();
	    					
	    					if(  !( Bytes.toString(t1).equals(Bytes.toString(t2))   )   ){
		    					List<KeyValue> ls2 = val_in.get(CSUB,Bytes.toBytes(""));
		    					List<KeyValue> lp2 = val_in.get(CPRE,Bytes.toBytes(""));
		    					List<KeyValue> lo2 = val_in.get(COBJ,Bytes.toBytes(""));
		    					KeyValue kvs2 = ls2.get( 0 ); 
		    					KeyValue kvp2=lp2.get( 0 ); 
		    					KeyValue kvo2=lo2.get( 0 ); 
		    					byte[] s2 = kvs2.getValue();
		    					byte[] p2 = kvp2.getValue();
		    					byte[] o2 = kvo2.getValue();

	    						Put row = new Put(Bytes.toBytes("row"+COUNT));
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Subject") , s1);
							batch.add(row);
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Predicate") , p1);
							batch.add(row);
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Object") , o1);
							batch.add(row);
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Subject") , s2);
							batch.add(row);
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Predicate") , p2);
							batch.add(row);
							row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Object") , o2);
							COUNT++;
	    					}
    					}
		    	}
		    	HTable temp_table = new HTable(conf, "tableTemp1_Raul");
		    	Object[] results = new Object[batch.size()];
			temp_table.batch(batch,results);
			batch.clear();
	   	}
	}
	//MAIN
	public static void main(String[] args) throws Exception { 
		//Naive
		//nested_Loop_Join("table1_Raul","table2_Raul","object", "object");
		//hash_joins("table1_Raul","table2_Raul","object", "object");
		
		//*TESTS*
		System.out.println("nested_Loop_Join starting....");
		System.out.println("Test1: ");
		long startTime = System.currentTimeMillis();
		//hash_Join("tableA_Raul","tableA_Raul","subject", "subject");
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		System.out.println("nested_Loop_Join starting....");
		System.out.println("Test2: ");
		startTime = System.currentTimeMillis();
		reduce_Join("tableA_Raul","tableA_Raul","subject", "subject");
		endTime   = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println(totalTime);
		System.out.println("Test3: ");
		startTime = System.currentTimeMillis();
		Reduce_opt_join("tableA_Raul","tableA_Raul","subject","predicate");
		endTime   = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println(totalTime);

	} 

	public static void reduce_join(String table_A, String table_B, String attrA, String attrB)throws Exception {
		union(table_A,table_B,attrA,attrB);

		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("tableTemp1_Raul"))){
				admin.disableTable(Bytes.toBytes("tableTemp1_Raul"));
				admin.deleteTable(Bytes.toBytes("tableTemp1_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("tableTemp1_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		admin.createTable(descriptor); 
    		} catch (Exception e) {
        		}
    		finally { 
    			admin.close(); 
		}	
		Job job = new Job(conf, "Hbase_Join");
		job.setJarByClass(hbase_mapred.Join.class);

//		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(
			"table_Raul", 
			scan, 
			MapperA.class, 
			Text.class,         // mapper output key
			Put.class,  // mapper output value
			job);
		//job.setReducerClass(Reducer1.class);    // reducer class
		//job.setNumReduceTasks(2);    // at least one, adjust as required
		TableMapReduceUtil.initTableReducerJob(
			"tableTemp_Raul",        // output table
			ReducerA.class,    // reducer class
			job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}
		public static void Reduce_opt_join(String table_A, String table_B, String attrA, String attrB)throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("table_Raul"))){
				admin.disableTable(Bytes.toBytes("table_Raul"));
				admin.deleteTable(Bytes.toBytes("table_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("table_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("subject")); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		descriptor.addFamily(new HColumnDescriptor("object"));
                    		descriptor.addFamily(new HColumnDescriptor("tag")); 
                    		admin.createTable(descriptor); 
    			admin.close(); 
		
                    		//A appended to B
			Scan scan = new Scan();
			HTable htable = new HTable(conf, Bytes.toBytes(table_A));
			ResultScanner rs = htable.getScanner(scan);
			ArrayList<Row> batch = new ArrayList<Row>();
			HTable temp_table = new HTable(conf, "table_Raul");
			int i =0;
			try {
				for (Result r = rs.next(); r != null; r = rs.next()) {
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("subject"), Bytes.toBytes("") ,r.getRow());
					for (KeyValue kv : r.raw()) {
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("") ,kv.getQualifier());
						row.add(Bytes.toBytes("object"), Bytes.toBytes("") , kv.getValue());
					}
					row.add(Bytes.toBytes("tag"), Bytes.toBytes("") ,Bytes.toBytes("A"));
					batch.add(row);
					i++;
				}	
			} finally {
				Object[] results = new Object[batch.size()];
				temp_table.batch(batch,results);
				rs.close();  // always close the ResultScanner!
				htable.close();
			}
    		} catch (Exception e) {
            		
        		}
    		finally { 
    			admin.close(); 
		}	
		admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("tableTemp1_Raul"))){
				admin.disableTable(Bytes.toBytes("tableTemp1_Raul"));
				admin.deleteTable(Bytes.toBytes("tableTemp1_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("tableTemp1_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		admin.createTable(descriptor); 
    		} catch (Exception e) {
        		}
    		finally { 
    			admin.close(); 
		}	
		Job job = new Job(conf, "Hbase_Join");
		job.setJarByClass(hbase_mapred.Join.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(
			"table_Raul", 
			scan, 
			MapperC.class, 
			Text.class,         // mapper output key
			Put.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(
			"tableTemp_Raul",        // output table
			ReducerA.class,    // reducer class
			job);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}

	}
	public static void union(String table_A, String table_B)throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("table_Raul"))){
				admin.disableTable(Bytes.toBytes("table_Raul"));
				admin.deleteTable(Bytes.toBytes("table_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("table_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("subject")); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		descriptor.addFamily(new HColumnDescriptor("object"));
                    		descriptor.addFamily(new HColumnDescriptor("tag")); 
                    		admin.createTable(descriptor); 
    			admin.close(); 
		
                    		//A appended to B
			Scan scan = new Scan();
			HTable htable = new HTable(conf, Bytes.toBytes(table_A));
			ResultScanner rs = htable.getScanner(scan);
			ArrayList<Row> batch = new ArrayList<Row>();
			HTable temp_table = new HTable(conf, "table_Raul");
			int i =0;
			try {
				for (Result r = rs.next(); r != null; r = rs.next()) {
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("subject"), Bytes.toBytes("") ,r.getRow());
					for (KeyValue kv : r.raw()) {
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("") ,kv.getQualifier());
						row.add(Bytes.toBytes("object"), Bytes.toBytes("") , kv.getValue());
					}
					row.add(Bytes.toBytes("tag"), Bytes.toBytes("") ,Bytes.toBytes("A"));
					batch.add(row);
					i++;
				}	
			} finally {
				rs.close();  // always close the ResultScanner!
				htable.close();
			}
			scan = new Scan();
			htable = new HTable(conf, Bytes.toBytes(table_B));
			rs = htable.getScanner(scan);
			try {
				for (Result r = rs.next(); r != null; r = rs.next()) {
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("subject"), Bytes.toBytes("") ,r.getRow());
					for (KeyValue kv : r.raw()) {
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("") ,kv.getQualifier());
						row.add(Bytes.toBytes("object"), Bytes.toBytes("") , kv.getValue());
					}
					row.add(Bytes.toBytes("tag"), Bytes.toBytes("") ,Bytes.toBytes("B"));
					batch.add(row);
					i++;
				}	
			} finally {
				Object[] results = new Object[batch.size()];
				temp_table.batch(batch,results);
				rs.close();  // always close the ResultScanner!
				htable.close();
			}
    		} catch (Exception e) {
            		
        		}
    		finally { 
    			admin.close(); 
		}	
	}

	public static void union(String table_A, String table_B, String attrA, String attrB)throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("table_Raul"))){
				admin.disableTable(Bytes.toBytes("table_Raul"));
				admin.deleteTable(Bytes.toBytes("table_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("table_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("subject")); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		descriptor.addFamily(new HColumnDescriptor("object"));
                    		descriptor.addFamily(new HColumnDescriptor("tag")); 
                    		descriptor.addFamily(new HColumnDescriptor("attr")); 
                    		admin.createTable(descriptor); 
    			admin.close(); 
		
                    		//A appended to B
			Scan scan = new Scan();
			HTable htable = new HTable(conf, Bytes.toBytes(table_A));
			ResultScanner rs = htable.getScanner(scan);
			ArrayList<Row> batch = new ArrayList<Row>();
			HTable temp_table = new HTable(conf, "table_Raul");
			int i =0;
			try {
				for (Result r = rs.next(); r != null; r = rs.next()) {
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("subject"), Bytes.toBytes("") ,r.getRow());
					for (KeyValue kv : r.raw()) {
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("") ,kv.getQualifier());
						row.add(Bytes.toBytes("object"), Bytes.toBytes("") , kv.getValue());
						if(attrA.equals("subject")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") ,r.getRow());
						}
						else if(attrA.equals("predicate")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") , kv.getQualifier());
						}
						else if(attrA.equals("object")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") , kv.getValue());
						}
					}
					row.add(Bytes.toBytes("tag"), Bytes.toBytes("") ,Bytes.toBytes("A"));
					batch.add(row);
					i++;
				}	
			} finally {
				rs.close();  // always close the ResultScanner!
				htable.close();
			}
			scan = new Scan();
			htable = new HTable(conf, Bytes.toBytes(table_B));
			rs = htable.getScanner(scan);
			try {
				for (Result r = rs.next(); r != null; r = rs.next()) {
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("subject"), Bytes.toBytes("") ,r.getRow());
					for (KeyValue kv : r.raw()) {
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("") ,kv.getQualifier());
						row.add(Bytes.toBytes("object"), Bytes.toBytes("") , kv.getValue());
						if(attrB.equals("subject")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") ,r.getRow());
						}
						else if(attrB.equals("predicate")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") , kv.getQualifier());
						}
						else if(attrB.equals("object")){
							row.add(Bytes.toBytes("attr"), Bytes.toBytes("") , kv.getValue());
						}
					}
					row.add(Bytes.toBytes("tag"), Bytes.toBytes("") ,Bytes.toBytes("B"));
					batch.add(row);
					i++;
				}	
			} finally {
				Object[] results = new Object[batch.size()];
				temp_table.batch(batch,results);
				rs.close();  // always close the ResultScanner!
				htable.close();
			}
    		} catch (Exception e) {
            		
        		}
    		finally { 
    			admin.close(); 
		}	
	}


	public static void hash_Join(String table_A, String table_B, String attrA, String attrB)throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("tableTemp_Raul"))){
				admin.disableTable(Bytes.toBytes("tableTemp_Raul"));
				admin.deleteTable(Bytes.toBytes("tableTemp_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("tableTemp_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		admin.createTable(descriptor); 
    		} catch (Exception e) {
            		
        		}
    		finally { 
    			admin.close(); 
		}	
		//NAIVE 				Join(A,B, A.s, B.p)
		HTable temp_table = new HTable(conf, "tableTemp_Raul");
		HTable htableA = new HTable(conf, table_A.getBytes());
		HTable htableB = new HTable(conf, table_B.getBytes());
		Scan scanA = new Scan();
		ResultScanner rsA = htableA.getScanner(scanA);
		Scan scanB = new Scan();
		byte[] s_tempA = new byte[0];
		byte[] p_tempA= new byte[0];
		byte[] o_tempA= new byte[0];
		String temp;
		byte[] s_tempB= new byte[0];
		byte[] p_tempB= new byte[0];
		byte[] o_tempB= new byte[0];

		ArrayList<Row> batch = new ArrayList<Row>();
		int i = 0;
		HashMap lookup = new HashMap( );

		ResultScanner rsB = htableB.getScanner(scanB);
		for (Result rB = rsB.next(); rB != null; rB = rsB.next()) {
			String tempB = "";
			s_tempB = rB.getRow();
			for (KeyValue kv : rB.raw()) {
				p_tempB = kv.getQualifier();
				o_tempB = kv.getValue();
			}
			if(attrA.equals("subject")){
				tempB = Bytes.toString(s_tempB);
			}
			else if(attrA.equals("predicate")){
				tempB =Bytes.toString(p_tempB);
			}
			else if(attrA.equals("object")){
				tempB = Bytes.toString(o_tempB);
			}
			if ( ! lookup.containsKey( tempB ) ) {
				ArrayList<RowC> list = new ArrayList<RowC>( );
				list.add(new RowC(s_tempB,p_tempB,o_tempB));
				lookup.put( tempB, list);
			}
			ArrayList<RowC> list =  (ArrayList<RowC>)lookup.get(tempB);
			list.add(new RowC(s_tempB,p_tempB,o_tempB));
			//lookup.put(tempB, new Row(s_tempB,p_tempB,o_tempB));
		}
		try {
			for (Result rA = rsA.next(); rA != null; rA = rsA.next()) {
				s_tempA = rA.getRow();
				for (KeyValue kv : rA.raw()) {
					p_tempA = kv.getQualifier();
					o_tempA = kv.getValue();
				}
				temp = "";
				if(attrA.equals("subject")){
					temp = Bytes.toString(s_tempA);
				}
				else if(attrA.equals("predicate")){
					temp = Bytes.toString(p_tempA);
				}
				else if(attrA.equals("object")){
					temp =Bytes.toString(o_tempA);
				}
				ArrayList<RowC> list = (ArrayList<RowC>)lookup.get(temp);
				for (RowC s : list){
					Put row = new Put(Bytes.toBytes("row"+i));
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Subject") , s_tempA);
					batch.add(row);
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Predicate") , p_tempA);
					batch.add(row);
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Object") , o_tempA);
					batch.add(row);
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Subject") , s_tempB);
					batch.add(row);
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Predicate") , p_tempB);
					batch.add(row);
					row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Object") , o_tempB);
					batch.add(row);
					row = null;
					i++;
				}
				if(batch.size() >= 10){
					Object[] results = new Object[batch.size()];
					temp_table.batch(batch,results);
					batch.clear();
				}
				rsB.close();  // always close the ResultScanner!
			}
		} finally {
			rsA.close();  // always close the ResultScanner!
		}
		try{
							if(batch.size() > 0){
						//System.out.println("Here");
						Object[] results = new Object[batch.size()];
						temp_table.batch(batch,results);
						batch.clear();
					}	
		}
		finally {
			htableA.close();
			htableB.close();
		}
	}
	public static void nested_Loop_Join(String table_A, String table_B, String attrA, String attrB)throws Exception {
		//*************APROACH 1************************
		HBaseAdmin admin = new HBaseAdmin(conf); 
		try{ 
	                    //Create Table 
			if(admin.tableExists(Bytes.toBytes("tableTemp_Raul"))){
				admin.disableTable(Bytes.toBytes("tableTemp_Raul"));
				admin.deleteTable(Bytes.toBytes("tableTemp_Raul"));
			}
          		          HTableDescriptor descriptor = new HTableDescriptor("tableTemp_Raul"); 
                    		descriptor.addFamily(new HColumnDescriptor("predicate")); 
                    		admin.createTable(descriptor); 
    		} catch (Exception e) {
            		
        		}
    		finally { 
    			admin.close(); 
		}	
		//NAIVE 				Join(A,B, A.s, B.p)
		HTable temp_table = new HTable(conf, "tableTemp_Raul");
		HTable htableA = new HTable(conf, table_A.getBytes());
		HTable htableB = new HTable(conf, table_B.getBytes());
		Scan scanA = new Scan();
		ResultScanner rsA = htableA.getScanner(scanA);
		Scan scanB = new Scan();
		byte[] s_tempA = new byte[0];
		byte[] p_tempA= new byte[0];
		byte[] o_tempA= new byte[0];
		String temp;
		byte[] s_tempB= new byte[0];
		byte[] p_tempB= new byte[0];
		byte[] o_tempB= new byte[0];

		ArrayList<Row> batch = new ArrayList<Row>();
		int i = 0;
		try {
			for (Result rA = rsA.next(); rA != null; rA = rsA.next()) {
				s_tempA = rA.getRow();
				for (KeyValue kv : rA.raw()) {
					p_tempA = kv.getQualifier();
					o_tempA = kv.getValue();
				}
				temp = "";
				if(attrA.equals("subject")){
					temp = Bytes.toString(s_tempA);
				}
				else if(attrA.equals("predicate")){
					temp = Bytes.toString(p_tempA);
				}
				else if(attrA.equals("object")){
					temp =Bytes.toString(o_tempA);
				}
				ResultScanner rsB = htableB.getScanner(scanB);
				for (Result rB = rsB.next(); rB != null; rB = rsB.next()) {
					String tempB = "";
					s_tempB = rB.getRow();
					for (KeyValue kv : rB.raw()) {
						p_tempB = kv.getQualifier();
						o_tempB = kv.getValue();
					}
					if(attrA.equals("subject")){
						tempB = Bytes.toString(s_tempB);
					}
					else if(attrA.equals("predicate")){
						tempB =Bytes.toString(p_tempB);
					}
					else if(attrA.equals("object")){
						tempB = Bytes.toString(o_tempB);
					}
					if(tempB.equals(temp)){
						Put row = new Put(Bytes.toBytes("row"+i));
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Subject") , s_tempA);
						batch.add(row);
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Predicate") , p_tempA);
						batch.add(row);
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("A_Object") , o_tempA);
						batch.add(row);
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Subject") , s_tempB);
						batch.add(row);
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Predicate") , p_tempB);
						batch.add(row);
						row.add(Bytes.toBytes("predicate"), Bytes.toBytes("B_Object") , o_tempB);
						batch.add(row);
						row = null;
						i++;
					}
					if(batch.size() >= 10){
						//System.out.println("Here1");
						Object[] results = new Object[batch.size()];
						temp_table.batch(batch,results);
						batch.clear();
					}
				}
				if(batch.size() >= 10){
						//System.out.println("Here1");
						Object[] results = new Object[batch.size()];
						temp_table.batch(batch,results);
						batch.clear();
				}
				rsB.close();  // always close the ResultScanner!
			}
		} finally {
			rsA.close();  // always close the ResultScanner!
		}
		try{
							if(batch.size() > 0){
						//System.out.println("Here");
						Object[] results = new Object[batch.size()];
						temp_table.batch(batch,results);
						batch.clear();
					}	
		}
		finally {
			htableA.close();
			htableB.close();
		}
	}
}

