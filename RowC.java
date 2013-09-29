package hbase_mapred;
public class RowC{
	byte [] subject;
	byte [] predicate;
	byte [] object;
	public RowC(byte[] s,byte[] p, byte[] o){	
		subject = s;
		predicate =p;
		object = o;
	}

}