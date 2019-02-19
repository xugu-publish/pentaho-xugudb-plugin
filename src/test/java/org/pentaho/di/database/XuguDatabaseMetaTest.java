package org.pentaho.di.database;

import org.pentaho.di.core.exception.KettleException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class XuguDatabaseMetaTest {
	//测试获取驱动
	@Test
	public void testDriverClass(){
		XuguDatabaseMeta dbMeta = new XuguDatabaseMeta();
		assertEquals( "com.xugu.cloudjdbc.Driver", dbMeta.getDriverClass() );
	}
	
	//测试组装Url
	@Test
	public void testGetUrl() throws KettleException{
		final String prefix = "jdbc:xugu://192.168.2.77:5138/testdb?char_set=GBK&auto_commit=false";
		XuguDatabaseMeta dbMeta = new XuguDatabaseMeta();
		// Test building url
		System.out.println("realURL get "+dbMeta.getURL("192.168.2.77", "5138", "testdb"));
		assertEquals( prefix, dbMeta.getURL( "192.168.2.77", "5138", "testdb"));
	}
}
