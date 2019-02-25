package org.pentaho.di.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.plugins.DatabasePluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;

/**
 * @author xugu-publish
 * @since 1.8
 *
 */
public class XuguDatabaseTest {
	@BeforeClass
	public static void setUpOnce() throws KettleException {
		DatabasePluginType dbPluginType = (DatabasePluginType) PluginRegistry.getInstance()
				.getPluginType(DatabasePluginType.class);
		dbPluginType.registerCustom(XuguDatabaseMeta.class, null, "Xugu", "Xugu", null, null);
		KettleClientEnvironment.init();
	}

	/**
	 * 测试读取数据
	 * 
	 * @throws KettleDatabaseException
	 * @throws SQLException
	 */
	@Test
	public void testReadDataIT() throws KettleDatabaseException, SQLException {
		XuguDatabaseMeta xuguMeta = new XuguDatabaseMeta();
		xuguMeta.setPluginId("Xugu");
		DatabaseMeta dbMeta = new DatabaseMeta();
		// 设置元数据信息
		dbMeta.setDatabaseInterface(xuguMeta);
		dbMeta.setHostname("192.168.2.77");
		dbMeta.setUsername("SYSDBA");
		dbMeta.setPassword("SYSDBA");
		dbMeta.setDBPort("5138");
		dbMeta.setDBName("testDB");
		System.out.println("TTTTTTTTTest connect " + dbMeta.getAttributes() + " " + dbMeta.getURL());

		// 连接数据库
		Database db = new Database(new LoggingObject(this), dbMeta);
		db.connect();

		// 获取测试结果集
		ResultSet result = db.openQuery("SELECT * FROM TESTTABLE4");

		// 检查结果集
		assertNotNull(result);
		Object[] row = db.getRow(result);
		RowMetaInterface meta = db.getMetaFromRow(row, result.getMetaData());
		assertNotNull(row);
		assertNotNull(meta);
		assertEquals(2, meta.size());
		assertEquals(0L, row[0]);
		assertEquals("TOM", row[1]);
		System.out.println("row1 " + row[0] + " " + row[1]);

		// 检测数据及其大小写是否正确
		row = db.getRow(result);
		assertNotNull(row);
		assertEquals(1L, row[0]);
		assertEquals("JAMEs", row[1]);
		System.out.println("row2 " + row[0] + " " + row[1]);

		row = db.getRow(result);
		assertNotNull(row);
		assertEquals(2L, row[0]);
		assertEquals("PAUL", row[1]);
		System.out.println("row3 " + row[0] + " " + row[1]);

		row = db.getRow(result);
		assertNotNull(row);
		System.out.println("row4 " + row[0] + " " + row[1]);

		row = db.getRow(result);
		assertNull(row);

		// 创建测试表
		// db.execStatement("Create table kettle_testTable(id ");

		// 获取测试字段数据类型映射
		// 数字类型
		ResultSet result1 = db.openQuery("SELECT * FROM type_test1");
		Object[] row1 = db.getRow(result1);
		RowMetaInterface meta1 = db.getMetaFromRow(row1, result1.getMetaData());
		// 字符及特殊类型
		ResultSet result2 = db.openQuery("SELECT * FROM type_test2");
		Object[] row2 = db.getRow(result2);
		RowMetaInterface meta2 = db.getMetaFromRow(row2, result2.getMetaData());
		// 时间类型
		ResultSet result3 = db.openQuery("SELECT * FROM type_test3");
		Object[] row3 = db.getRow(result3);
		RowMetaInterface meta3 = db.getMetaFromRow(row3, result3.getMetaData());

		// 检测字段类型映射
		String[] namesAndTypes = meta1.getFieldNamesAndTypes(20);
		System.out.println("Table1");
		for (int i = 0; i < namesAndTypes.length; i++) {
			System.out.println("col" + i + " " + namesAndTypes[i]);
		}
		namesAndTypes = meta2.getFieldNamesAndTypes(20);
		System.out.println("Table2");
		for (int i = 0; i < namesAndTypes.length; i++) {
			System.out.println("col" + i + " " + namesAndTypes[i]);
		}
		namesAndTypes = meta3.getFieldNamesAndTypes(20);
		System.out.println("Table3");
		for (int i = 0; i < namesAndTypes.length; i++) {
			System.out.println("col" + i + " " + namesAndTypes[i]);
		}

		// 测试字段大小写
		ResultSet result4 = db.openQuery("SELECT * FROM field_test");
		Object[] row4 = db.getRow(result4);
		RowMetaInterface meta4 = db.getMetaFromRow(row4, result4.getMetaData());

		namesAndTypes = meta4.getFieldNamesAndTypes(20);
		System.out.println("Table4");
		for (int i = 0; i < namesAndTypes.length; i++) {
			System.out.println("col" + i + " " + namesAndTypes[i]);
		}

		db.disconnect();
	}
}
