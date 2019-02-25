package org.pentaho.di.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;

//该注解指明该类为数据库插件类
@DatabaseMetaPlugin(type = "Xugu", typeDescription = "Xugu Database")

/**
 * Pentaho Kettle数据库插件(虚谷数据库)
 * 
 * @author xugu-publish
 * @sinc 1.8
 * @date 2019-02-23
 */
public class XuguDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

	/**
	 * VARCHAR类型数据最大长度(虚谷VARCHAR最大长度依赖行64K的限制，无发精确确定，依据Oracle最大长度界定(字节最大长度))
	 */
	private static final int INTEGER_LIMIT = 9;
	
	private static final int BIGINT_LIMIT = 19;
	
	private static final int VARCHAR_LIMIT = 4000;

	/**
	 * 返回连接类型 目前暂时仅支持JDBC(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getAccessTypeList()
	 */
	@Override
	public int[] getAccessTypeList() {
		return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
	}

	/**
	 * 返回驱动类(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getDriverClass()
	 */
	@Override
	public String getDriverClass() {
		return "com.xugu.cloudjdbc.Driver";
	}

	/**
	 * 返回数据库连接url(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getURL(java.lang.String,
	 *      java.lang.String, java.lang.String)
	 */
	@Override
	public String getURL(String hostName, String port, String databaseName) {

		if (Utils.isEmpty(port)) {
			return "jdbc:xugu://" + hostName + "/" + databaseName;
		} else {
			return "jdbc:xugu://" + hostName + ":" + port + "/" + databaseName;
		}
	}

	/**
	 * DEFAULT SETTINGS FOR XUGU DATABASES
	 * ********************************************************************************
	 */
	/**
	 * 返回默认端口(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getDefaultDatabasePort()
	 */
	@Override
	public int getDefaultDatabasePort() {
		if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
			return 5138;
		}
		return -1;
	}

	/**
	 * @return The extra option separator in database URL for this platform (usually
	 *         this is semicolon ; )
	 */
	@Override
	public String getExtraOptionSeparator() {
		return "&";
	}

	/**
	 * @return This indicator separates the normal URL from the options
	 */
	@Override
	public String getExtraOptionIndicator() {
		return "?";
	}

	/**
	 * 为查询限制行数(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getLimitClause(int)
	 */
	@Override
	public String getLimitClause(int nrRows) {
		return " LIMIT " + nrRows;
	}

	/**
	 * 返回一个关于表的最小查询语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getSQLQueryFields(java.lang.
	 *      String)
	 */
	@Override
	public String getSQLQueryFields(String tableName) {
		return "SELECT * FROM " + tableName + " LIMIT 0";
	}

	/**
	 * Get the SQL to get the next value of a sequence.
	 *
	 * @param sequenceName The sequence name
	 * @return the SQL to get the next value of a sequence. (Oracle only)
	 */
	@Override
	public String getSQLNextSequenceValue(String sequenceName) {
		return "SELECT " + sequenceName + ".nextval FROM dual";
	}

	/**
	 * Check if a sequence exists.
	 *
	 * @param sequenceName The sequence to check
	 * @return The SQL to get the name of the sequence back from the databases data
	 *         dictionary
	 */
	@Override
	public String getSQLSequenceExists(String sequenceName) {
		int dotPos = sequenceName.indexOf('.');
		String sql = "";
		if (dotPos == -1) {
			// if schema is not specified try to get sequence which belongs to current user
			sql = "SELECT * FROM USER_SEQUENCES WHERE SEQ_NAME = '" + sequenceName.toUpperCase() + "'";
		} else {
			String schemaName = sequenceName.substring(0, dotPos);
			String seqName = sequenceName.substring(dotPos + 1);
			sql = "SELECT SEQ.* FROM USER_SEQUENCES SEQ LEFT JOIN USER_SCHEMAS SCH ON SEQ.SCHEMA_ID = SCH.SCHEMA_ID WHERE SEQ.SEQ_NAME = '"
					+ seqName.toUpperCase() + "' AND SCH.SCHEMA_NAME = '" + schemaName.toUpperCase() + "'";
		}
		return sql;
	}

	/**
	 * 通过查询语句判断表是否存在 若抛出异常则表不存在(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getSQLTableExists(java.lang.
	 *      String)
	 */
	@Override
	public String getSQLTableExists(String tableName) {
		return getSQLQueryFields(tableName);
	}

	/**
	 * 返回一个关于列的最小查询语句
	 */
	public String getSQLQueryColumnFields(String columnName, String tableName) {
		return "SELECT " + columnName + " FROM " + tableName + " LIMIT 0";
	}

	/**
	 * 通过查询语句判断列是否存在 若抛出异常则列不存在(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getSQLColumnExists(java.lang.
	 *      String, java.lang.String)
	 */
	@Override
	public String getSQLColumnExists(String columnName, String tableName) {
		return getSQLQueryColumnFields(columnName, tableName);
	}

	/**
	 * @return the SQL to retrieve the list of schemas or null if the JDBC metadata
	 *         needs to be used.
	 */
	@Override
	public String getSQLListOfSchemas() {
		return "SELECT SCHEMA_NAME FROM ALL_SCHEMAS";
	}

	/**
	 * @return The maximum number of columns in a database, <=0 means: no known
	 *         limit
	 */
	@Override
	public int getMaxColumnsInIndex() {
		return 0;
	}

	/**
	 * @return true if the database supports sequences
	 */
	@Override
	public boolean supportsSequences() {
		return true;
	}

	/**
	 * 是否支持同义词(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#supportsSynonyms()
	 */
	@Override
	public boolean supportsSynonyms() {
		return true;
	}

	/**
	 * @return The SQL on this database to get a list of sequences.
	 */
	@Override
	public String getSQLListOfSequences() {
		return "SELECT SEQ_NAME FROM all_sequences";
	}

	/**
	 * @return true if the database supports a boolean, bit, logical, ... datatype
	 *         The default is false: map to a string.
	 */
	@Override
	public boolean supportsBooleanDataType() {
		return true;
	}

	/**
	 * @return true if the database supports the Timestamp data type (nanosecond
	 *         precision and all)
	 */
	@Override
	public boolean supportsTimestampDataType() {
		return true;
	}

	/**
	 * Oracle does not support a construct like 'drop table if exists', which is
	 * apparently legal syntax in many other RDBMSs. So we need to implement the
	 * same behavior and avoid throwing 'table does not exist' exception.
	 *
	 * @param tableName Name of the table to drop
	 * @return 'drop table if exists'-like statement for Oracle
	 */
	@Override
	public String getDropTableIfExistsStatement(String tableName) {
		return "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + tableName + "';"
				+ " EXCEPTION WHEN OTHERS THEN IF SQLCODE != 192 THEN RAISE; END IF; END;";
	}

	/**
	 * 返回字段定义 用于组装sql语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getFieldDefinition(org.pentaho
	 *      .di.core.row.ValueMetaInterface, java.lang.String, java.lang.String,
	 *      boolean, boolean, boolean)
	 */
	@Override
	public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean useAutoinc,
			boolean addFieldName, boolean addCr) {
		String retval = "";
		// 获取字段信息
		String fieldName = v.getName();
		// clob类型长度处理 长度一致则不需要处理
		if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
			v.setLength(getMaxTextFieldLength());
		}
		// 字段长度
		int length = v.getLength();
		// 字段精度
		int precision = v.getPrecision();

		// 新增一列
		if (addFieldName) {
			retval += fieldName + " ";
		}

		// 字段类型
		int type = v.getType();
		switch (type) {
		case ValueMetaInterface.TYPE_DATE:
			retval += "DATE";
			break;
		case ValueMetaInterface.TYPE_TIMESTAMP:
			retval += "DATETIME";
			break;
		case ValueMetaInterface.TYPE_BOOLEAN:
			retval += "BOOLEAN";
			break;
		case ValueMetaInterface.TYPE_INTEGER:
			retval += "INTEGER";
			break;
		case ValueMetaInterface.TYPE_NUMBER:
			// 如果修改的是键 则将其类型直接设为BigInt
			if (fieldName.equalsIgnoreCase(tk) || fieldName.equalsIgnoreCase(pk)) {
				// 设为自增
				if (useAutoinc) {
					retval += "BIGINT identity(1,1) NOT NULL PRIMARY KEY";
				} else {
					retval += "BIGINT NOT NULL PRIMARY KEY";
				}
			} else {
				// 整型数据
				if (precision == 0) {
					if (length > INTEGER_LIMIT) {
						// 10-18位整数 设为BIGINT
						if (length < BIGINT_LIMIT) {
							retval += "BIGINT";
						}
						// 19位及以上设为NUMERIC
						else {
							retval += "NUMERIC(" + length + ")";
						}
					} else {
						retval += "INTEGER";
					}
				}
				// 浮点型数据
				else {
					retval += "NUMERIC(" + length;
					if (precision > 0) {
						retval += ", " + precision;
					}
					retval += ")";
				}
			}
			break;
		case ValueMetaInterface.TYPE_BIGNUMBER:
			retval += "NUMBER";
			if (length > 0) {
				retval += "(" + length;
				if (precision > 0) {
					retval += ", " + precision;
				}
				retval += ")";
			}
			break;
		case ValueMetaInterface.TYPE_STRING:
			if (length < 1) {
				retval += "VARCHAR(-1)";
			} else if (length >= VARCHAR_LIMIT) {
				retval += "CLOB";
			} else {
				retval += "VARCHAR(" + length + ")";
			}
			break;
		case ValueMetaInterface.TYPE_BINARY:
			retval += "BINARY";
			break;
		default:
			retval += " UNKNOWN";
			break;
		}

		if (addCr) {
			retval += Const.CR;
		}

		return retval;
	}

	/**
	 * 构造增加列的Sql语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getAddColumnStatement(java.
	 *      lang.String, org.pentaho.di.core.row.ValueMetaInterface,
	 *      java.lang.String, boolean, java.lang.String, boolean)
	 */
	@Override
	public String getAddColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean useAutoinc,
			String pk, boolean semicolon) {
		String s = "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
		System.out.println("ADD..." + s);
		return "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
	}

	/**
	 * 构造修改列的Sql语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getModifyColumnStatement(java.
	 *      lang.String, org.pentaho.di.core.row.ValueMetaInterface,
	 *      java.lang.String, boolean, java.lang.String, boolean)
	 */
	@Override
	public String getModifyColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean useAutoinc,
			String pk, boolean semicolon) {
		String s = "ALTER TABLE " + tableName + " ALTER COLUMN "
				+ getFieldDefinition(v, tk, pk, useAutoinc, true, false);
		System.out.println("Modify..." + s);
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + getFieldDefinition(v, tk, pk, useAutoinc, true, false);
	}

	/**
	 * 构造删除列的Sql语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getDropColumnStatement(java.
	 *      lang.String, org.pentaho.di.core.row.ValueMetaInterface,
	 *      java.lang.String, boolean, java.lang.String, boolean)
	 */
	@Override
	public String getDropColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean useAutoinc,
			String pk, boolean semicolon) {
		String s = "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
		System.out.println("Drop..." + s);
		return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
	}

	/**
	 * 获取保留字(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getReservedWords()
	 */
	@Override
	public String[] getReservedWords() {
		return new String[] { "ABORT", "ABOVE", "ABSOLUTE", "ACCESS", "ACCOUNT", "ACTION", "ADD", "AFTER", "AGGREGATE",
				"ALL", "ALTER", "ANALYSE", "ANALYZE", "AND", "ANY", "AOVERLAPS", "APPEND", "ARCHIVELOG", "ARE", "ARRAY",
				"AS", "ASC", "AT", "AUDIT", "AUDITOR", "AUTHID", "AUTHORIZATION", "AUTOBACKUP", "BACKWARD", "BADFILE",
				"BCONTAINS", "BEFORE", "BEGIN", "BETWEEN", "BINARY", "BINTERSECTS", "BIT", "BLOCK", "BLOCKS", "BODY",
				"BOTH", "BOUND", "BOVERLAPS", "BREAK", "BUFFER_POOL", "BUILD", "BULK", "BWITHIN", "BYCACHE", "CALL",
				"CASCADE", "CASE", "CAST", "CATCH", "CATEGORY", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS",
				"CHECK", "CHECKPOINT", "CHUNK", "CLOSE", "CLUSTER", "COALESCE", "COLLATE", "COLLECT", "COLUMN",
				"COMMENT", "COMMIT", "COMMITTED", "COMPLETE", "COMPRESS", "COMPUTE", "CONNECT", "CONSTANT",
				"CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR", "CONTAINS", "CONTEXT", "CONTINUE", "COPY", "CORRESPONDING",
				"CREATE", "CREATEDB", "CREATEUSER", "CROSSCROSSES", "CUBE", "CURRENT", "CURSOR", "CYCLEDATABASE",
				"DATAFILE", "DATE", "DATETIME", "DAY", "DBA", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DECODE",
				"DECRYPT", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DELIMITED", "DELIMITERS", "DEMAND", "DESC",
				"DESCRIBE", "DETERMINISTIC", "DIR", "DISABLE", "DISASSEMBLE", "DISCORDFILE", "DISJOINT", "DISTINCT",
				"DO", "DOMAIN", "DOUBLE", "DRIVEN", "DROPEACH", "ELEMENT", "ELSE", "ELSEIF", "ELSIF", "ENABLE",
				"ENCODING", "ENCRYPT", "ENCRYPTOR", "END", "ENDCASE", "ENDFOR", "ENDIF", "ENDLOOP", "EQUALS", "ESCAPE",
				"EVERY", "EXCEPT", "EXCEPTION", "EXCEPTIONS", "EXCLUSIVE", "EXEC", "EXECUTE", "EXISTS", "EXIT",
				"EXPIRE", "EXPLAIN", "EXPORT", "EXTEND", "EXTERNAL", "EXTRACTFALSE", "FAST", "FETCH", "FIELD", "FIELDS",
				"FILTER", "FINAL", "FINALLY", "FIRST", "FLOAT", "FOLLOWING", "FOR", "FORALL", "FORCE", "FOREIGN",
				"FORWARD", "FOUND", "FREELIST", "FREELISTS", "FROM", "FULL", "FUNCTIONGENERATED", "GET", "GLOBAL",
				"GOTO", "GRANT", "GREATEST", "GROUP", "GROUPING", "GROUPSHANDLER", "HASH", "HAVING", "HEAP", "HIDE",
				"HOTSPOT", "HOURIDENTIFIED", "IDENTIFIER", "IDENTITY", "IF", "ILIKE", "IMMEDIATE", "IMPORT", "IN",
				"INCLUDE", "INCREMENT", "INDEX", "INDEXTYPE", "INDICATOR", "INDICES", "INHERITS", "INIT", "INITIAL",
				"INITIALLY", "INITRANS", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INSTANTIABLE", "INSTEAD",
				"INTERSECTINTERSECTS", "INTERVAL", "INTO", "IO", "IS", "ISNULL", "ISOLATION", "ISOPENJOB", "JOINK",
				"KEEP", "KEY", "KEYSETLABEL", "LANGUAGE", "LAST", "LEADING", "LEAST", "LEAVE", "LEFT", "LEFTOF",
				"LENGTH", "LESS", "LEVEL", "LEVELS", "LEXER", "LIBRARY", "LIKE", "LIMIT", "LINK", "LIST", "LISTEN",
				"LOAD", "LOB", "LOCAL", "LOCATION", "LOCATOR", "LOCK", "LOGFILE", "LOGGING", "LOGIN", "LOGOUT", "LOOP",
				"LOVERLAPSM", "MATCH", "MATERIALIZED", "MAX", "MAXEXTENTS", "MAXSIZE", "MAXTRANS", "MAXVALUE",
				"MAXVALUES", "MEMBER", "MEMORY", "MERGEMINEXTENTS", "MINUS", "MINUTE", "MINVALUE", "MISSING", "MODE",
				"MODIFY", "MONTH", "MOVEMENTNAME", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NESTED", "NEW", "NEWLINE",
				"NEXT", "NO", "NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCREATEDB", "NOCREATEUSER",
				"NOCYCLE", "NODE", "NOFORCE", "NOFOUND", "NOLOGGING", "NONE", "NOORDER", "NOPARALLEL", "NOT",
				"NOTFOUND", "NOTHING", "NOTIFY", "NOTNULL", "NOVALIDATE", "NOWAIT", "NULL", "NULLIF", "NULLS", "NUMBER",
				"NUMERIC", "NVARCHAR", "NVARCHAR2", "NVL", "NVL2", "OBJECT", "OF", "OFF", "OFFLINE", "OFFSET",
				"OIDINDEX", "OIDS", "OLD", "ON", "ONLINE", "ONLY", "OPEN", "OPERATOR", "OPTION", "OR", "ORDER",
				"ORGANIZATION", "OTHERVALUES", "OUT", "OUTER", "OVER", "OVERLAPS", "OWNERPACKAGE", "PARALLEL",
				"PARAMETERS", "PARTIAL", "PARTITION", "PARTITIONS", "PASSWORD", "PCTFREE", "PCTINCREASE", "PCTUSED",
				"PCTVERSION", "PERIOD", "POLICY", "PRAGMA", "PREBUILT", "PRECEDING", "PRECISION", "PREPARE", "PRESERVE",
				"PRIMARY", "PRIOR", "PRIORITY", "PRIVILEGES", "PROCEDURAL", "PROCEDURE", "PROTECTED", "PUBLICQUERY",
				"QUOTARAISE", "RANGE", "RAW", "READ", "READS", "REBUILD", "RECOMPILE", "RECORD", "RECORDS", "RECYCLE",
				"REDUCED", "REF", "REFERENCES", "REFERENCING", "REFRESH", "REINDEX", "RELATIVE", "RENAME", "REPEATABLE",
				"REPLACE", "REPLICATION", "RESOURCE", "RESTART", "RESTORE", "RESTRICT", "RESULT", "RETURN", "RETURNING",
				"REVERSE", "REVOKE", "REWRITE", "RIGHT", "RIGHTOF", "ROLE", "ROLLBACK", "ROLLUP", "ROVERLAPS", "ROW",
				"ROWCOUNT", "ROWID", "ROWS", "ROWTYPE", "RULE", "RUNSAVEPOINT", "SCHEMA", "SCROLL", "SECOND", "SEGMENT",
				"SELECT", "SELF", "SEQUENCE", "SERIALIZABLE", "SESSION", "SET", "SETOF", "SETS", "SHARE", "SHOW",
				"SHUTDOWN", "SIBLINGS", "SIZE", "SLOW", "SNAPSHOT", "SOME", "SPATIAL", "SPLIT", "SSO", "STANDBY",
				"START", "STATEMENT", "STATIC", "STATISTICS", "STEP", "STOP", "STORAGE", "STORE", "STREAM",
				"SUBPARTITIONSUBPARTITIONS", "SUBTYPE", "SUCCESSFUL", "SYNONYM", "SYSTEMTABLE", "TABLESPACE", "TEMP",
				"TEMPLATE", "TEMPORARY", "TERMINATED", "THAN", "THEN", "THROW", "TIME", "TIMESTAMP", "TO", "TOP",
				"TOPOVERLAPS", "TOUCHES", "TRACE", "TRAILING", "TRAN", "TRANSACTION", "TRIGGER", "TRUE", "TRUNCATE",
				"TRUSTED", "TRY", "TYPEUNBOUNDED", "UNDER", "UNDO", "UNIFORM", "UNION", "UNIQUE", "UNLIMITED",
				"UNLISTEN", "UNLOCK", "UNPROTECTED", "UNTIL", "UOVERLAPS", "UPDATE", "USE", "USER", "USINGVACUUM",
				"VALID", "VALIDATE", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARRAY", "VARYING", "VERBOSE",
				"VERSION", "VIEW", "VOCABLEWAIT", "WHEN", "WHENEVER", "WHERE", "WHILE", "WITH", "WITHIN", "WITHOUT",
				"WORK", "WRITE", "XML", "YEAR", "ZONE" };
	}

	/**
	 * 获取在线帮助文档地址ַ(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#getExtraOptionsHelpText()
	 */
	@Override
	public String getExtraOptionsHelpText() {
		return "";
	}

	/**
	 * 获取所需的jar包(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.DatabaseInterface#getUsedLibraries()
	 */
	@Override
	public String[] getUsedLibraries() {
		return new String[] { "cloudjdbc-10.0.jar" };
	}

	/**
	 * 格式化Sql语句(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#quoteSQLString(java.lang.
	 *      String)
	 */
	@Override
	public String quoteSQLString(String string) {
		string = string.replaceAll("'", "\\\\'");
		string = string.replaceAll("\\n", "\\\\n");
		string = string.replaceAll("\\r", "\\\\r");
		return "'" + string + "'";
	}

	/**
	 * 可否释放SavePoint(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#releaseSavepoint()
	 */
	@Override
	public boolean releaseSavepoint() {
		return false;
	}

	/**
	 * 是否支持在批处理过程中进行错误处理(non-Javadoc)
	 * 
	 * @see org.pentaho.di.core.database.BaseDatabaseMeta#
	 *      supportsErrorHandlingOnBatchUpdates()
	 */
	@Override
	public boolean supportsErrorHandlingOnBatchUpdates() {
		return true;
	}

}
