package org.pentaho.di.database;

import org.pentaho.di.core.exception.KettleException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * @author xugu-publish
 * @since 1.8
 *
 */
public class XuguDatabaseMetaTest {

	/**
	 * 测试获取虚谷JDBC驱动类
	 */
	@Test
	public void testDriverClass() {
		XuguDatabaseMeta dbMeta = new XuguDatabaseMeta();
		assertEquals("com.xugu.cloudjdbc.Driver", dbMeta.getDriverClass());
	}

	/**
	 * 测试获取虚谷JDBC URL信息
	 * 
	 * @throws KettleException
	 */
	@Test
	public void testGetUrl() throws KettleException {
		final String prefix = "jdbc:xugu://192.168.2.76:5151/SYSTEM";
		XuguDatabaseMeta dbMeta = new XuguDatabaseMeta();
		// Test building url
		System.out.println("realURL get " + dbMeta.getURL("192.168.2.76", "5151", "SYSTEM"));
		assertEquals(prefix, dbMeta.getURL("192.168.2.76", "5151", "SYSTEM"));
	}
}
