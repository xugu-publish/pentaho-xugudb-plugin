package org.pentaho.di.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
//import java.sql.ResultSet;

//import com.google.common.collect.Sets;
//import org.pentaho.di.core.Const;
//import org.pentaho.di.core.util.Utils;
//import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
//import org.pentaho.di.core.row.ValueMetaInterface;
//import org.pentaho.di.core.variables.VariableSpace;

//the annotation allows PDI to recognize this class as a database plug-in 
@DatabaseMetaPlugin(
type = "Xugu",
typeDescription = "Xugu"
)

public class XuguDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface{
	 private static final String STRICT_BIGNUMBER_INTERPRETATION = "STRICT_NUMBER_38_INTERPRETATION";
	/**
	   * @return true if using strict number(38) interpretation
	   */
	  public boolean strictBigNumberInterpretation() {
	    return "Y".equalsIgnoreCase( getAttributes().getProperty( STRICT_BIGNUMBER_INTERPRETATION, "N" ) );
	  }

	  /**
	   * @param  strictBigNumberInterpretation true if use strict number(38) interpretation
	   */
	  public void setStrictBigNumberInterpretation( boolean strictBigNumberInterpretation ) {
	    getAttributes().setProperty( STRICT_BIGNUMBER_INTERPRETATION, strictBigNumberInterpretation ? "Y" : "N" );
	  }
		@Override
		public int[] getAccessTypeList(){
			return new int[]{ DatabaseMeta.TYPE_ACCESS_NATIVE};
		}
		
		@Override
		public int  getDefaultDatabasePort(){
			return 5138;
		}
		
		@Override
		public String getLimitClause(int nrRows){
			return " LIMIT " + nrRows;// mysql
		}
		
		@Override
		// 返回一个最小启动sql 用于确定结果集位于哪张表
		public String getSQLQueryFields(String tableName){
			return "SELECT * FROM " + tableName + " LIMIT 0";
		}
		
		@Override
		// 调用getSQLQueryFields 若有异常则证明表不存在
		public String getSQLTableExists(String tableName){
			return getSQLQueryFields(tableName);
		}
		

		// 返回一个最小sql 用于测试列是否存在于表中 
		public String getSQLQueryColumnFields(String columnName, String tableName){
			return "SELECT " + columnName + " FROM " + tableName + " LIMIT 0";
		}
		
		@Override
		// 调用getSQLQueryColumnFields 若有异常则证明列不存在
		public String getSQLColumnExists(String columnName, String tableName){
			return getSQLQueryColumnFields(columnName, tableName);
		}
		
		//@Override
		public String getDriverClass(){
			return "com.xugu.cloudjdbc.Driver";
		}
		
		//@Override
		public String getURL(String hostName, String port, String databaseName){
			return "jdbc:xugu://" + hostName + ":" + port + "/" +databaseName;
		}
		
		@Override
		public boolean supportsSynonyms(){
			return true;
		}
		
		//@Override
		public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldName, boolean add_cr){
			String retval = "";
			
			//获取字段信息
			String fieldName = v.getName();
			//clob类型长度处理 长度一致则不需要处理
//			if(v.getLength() == DatabaseMeta.CLOB_LENGTH) {
//				v.setLength(getMaxTextFieldLength());
//			}
			int length = v.getLength();
			int precision = v.getPrecision();
			int type = v.getType();
			
			// 新增一列
			if(add_fieldName) {
				retval += fieldName + " ";
			}
			
			switch(type) {
				case ValueMetaInterface.TYPE_DATE:
					retval += "DATE";
					break;
				case ValueMetaInterface.TYPE_TIMESTAMP:
					retval += "DATETIME";
					break;
				case ValueMetaInterface.TYPE_BOOLEAN:
					retval += "BOOLEAN";
					break;
				case ValueMetaInterface.TYPE_NUMBER:
				case ValueMetaInterface.TYPE_INTEGER:
				case ValueMetaInterface.TYPE_BIGNUMBER:
					// 如果修改的是键 则将其类型直接设为BigInt
					if(fieldName.equalsIgnoreCase(tk)||
							fieldName.equalsIgnoreCase(pk)) {
						// 设为自增
						if(use_autoinc) {
							retval += "BIGINT identity(0,1) NOT NULL PRIMARY KEY";
						}else {
							retval += "BIGINT NOT NULL PRIMARY KEY";
						}
					}else {
						// 整型数据
						if(precision==0) {
							if(length>9) {
								// 10-18位整数 设为BigInt
								if(length<19) {
									retval += "BIGINT";
								}
								// 19位及以上设为DECIMAL
								else {
									retval += "NUMERIC(" + length + ")";
								}
							}else {
								retval += "INTEGER";
							}
						}
						// 浮点型数据
						else {
							if(length>15) {
								retval += "NUMERIC(" + length;
								if(precision>0) {
									retval += ", " + precision;
								}
								retval += ")";
							}else {
								retval += "DOUBLE";
							}
						}
					}
					break;
				case ValueMetaInterface.TYPE_STRING:
					if(length == 1) {
						retval += "CHAR(1)";
					}else if(length < 65536) {
						retval += "VARCHAR(" + length + ")";
					}else {
						retval += "CLOB";
					}
					break;
				case ValueMetaInterface.TYPE_BINARY:
					retval += "BINARY";
					break;
			}
			
			if(add_cr) {
				retval += Const.CR;
			}
			
			return retval;
		}
		
		//@Override
		public String getAddColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon){
			String s =  "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
			System.out.println("ADD..."+s);
			return "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
		}
		
		//@Override 
		public String getModifyColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon){		
			String s = "ALTER TABLE " + tableName +" ALTER COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
			System.out.println("Modify..."+s);
			return "ALTER TABLE " + tableName +" ALTER COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
		}
		
		@Override
		public String getDropColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
			String s = "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
			System.out.println("Drop..."+s);
			return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
		}
		
		@Override
		public String[] getReservedWords(){
			return new String[]{
				"ABORT","ABOVE","ABSOLUTE","ACCESS","ACCOUNT","ACTION",
				"ADD","AFTER","AGGREGATE","ALL","ALTER","ANALYSE","ANALYZE",
				"AND","ANY","AOVERLAPS","APPEND","ARCHIVELOG","ARE","ARRAY",
				"AS","ASC","AT","AUDIT","AUDITOR", "AUTHID","AUTHORIZATION",
				"AUTOBACKUP","BACKWARD","BADFILE", "BCONTAINS", "BEFORE", "BEGIN",
				"BETWEEN", "BINARY", "BINTERSECTS", "BIT", "BLOCK", "BLOCKS",
				"BODY", "BOTH", "BOUND", "BOVERLAPS", "BREAK", "BUFFER_POOL", 
				"BUILD", "BULK", "BWITHIN", "BYCACHE", "CALL", "CASCADE", "CASE", 
				"CAST", "CATCH", "CATEGORY", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS", 
				"CHECK", "CHECKPOINT", "CHUNK", "CLOSE", "CLUSTER", "COALESCE", 
				"COLLATE", "COLLECT", "COLUMN", "COMMENT", "COMMIT", "COMMITTED", 
				"COMPLETE", "COMPRESS", "COMPUTE", "CONNECT",  "CONSTANT", "CONSTRAINT", 
				"CONSTRAINTS", "CONSTRUCTOR", "CONTAINS", "CONTEXT", "CONTINUE", 
				"COPY", "CORRESPONDING", "CREATE", "CREATEDB", "CREATEUSER", 
				"CROSSCROSSES", "CUBE", "CURRENT", "CURSOR", "CYCLEDATABASE", "DATAFILE", 
				"DATE", "DATETIME", "DAY", "DBA", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", 
				"DECODE", "DECRYPT", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", 
				"DELIMITED", "DELIMITERS", "DEMAND", "DESC", "DESCRIBE", "DETERMINISTIC",
				"DIR", "DISABLE", "DISASSEMBLE", "DISCORDFILE", "DISJOINT", "DISTINCT", 
				"DO", "DOMAIN", "DOUBLE", "DRIVEN", "DROPEACH", "ELEMENT", "ELSE", "ELSEIF",
				"ELSIF", "ENABLE", "ENCODING", 
				"ENCRYPT", "ENCRYPTOR", "END", "ENDCASE", "ENDFOR", "ENDIF", "ENDLOOP", "EQUALS",
				"ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXCEPTIONS", "EXCLUSIVE", 
				"EXEC", "EXECUTE", "EXISTS", "EXIT", "EXPIRE", "EXPLAIN", "EXPORT", 
				"EXTEND", "EXTERNAL", "EXTRACTFALSE", "FAST", "FETCH", "FIELD", "FIELDS",
				"FILTER", "FINAL", "FINALLY", 
				"FIRST", "FLOAT", "FOLLOWING", "FOR", "FORALL", "FORCE", "FOREIGN", 
				"FORWARD", "FOUND", "FREELIST", "FREELISTS", "FROM", "FULL", "FUNCTIONGENERATED", 
				"GET", "GLOBAL", "GOTO", "GRANT", "GREATEST", "GROUP", "GROUPING", 
				"GROUPSHANDLER", "HASH", "HAVING", "HEAP", "HIDE", "HOTSPOT", "HOURIDENTIFIED",
				"IDENTIFIER", "IDENTITY", "IF", "ILIKE", "IMMEDIATE", "IMPORT", "IN",
				"INCLUDE", "INCREMENT", "INDEX", "INDEXTYPE", "INDICATOR",
				"INDICES", "INHERITS", "INIT", "INITIAL", "INITIALLY", "INITRANS", "INNER",
				"INOUT", "INSENSITIVE", "INSERT", "INSTANTIABLE", "INSTEAD",
				"INTERSECTINTERSECTS", "INTERVAL", "INTO", "IO", "IS", "ISNULL",
				"ISOLATION", "ISOPENJOB", "JOINK", "KEEP",  "KEY",  "KEYSETLABEL",
				"LANGUAGE", "LAST", "LEADING", "LEAST", "LEAVE", "LEFT", "LEFTOF",
				"LENGTH", "LESS", "LEVEL", "LEVELS", "LEXER", "LIBRARY", "LIKE", "LIMIT", 
				"LINK", "LIST", "LISTEN", "LOAD", "LOB", "LOCAL", "LOCATION", "LOCATOR",
				"LOCK", "LOGFILE", "LOGGING", "LOGIN", "LOGOUT", "LOOP", "LOVERLAPSM",
				"MATCH", "MATERIALIZED", "MAX", "MAXEXTENTS", "MAXSIZE", "MAXTRANS", 
				"MAXVALUE", "MAXVALUES", "MEMBER", "MEMORY", "MERGEMINEXTENTS", "MINUS",
				"MINUTE", "MINVALUE", "MISSING", "MODE", "MODIFY", "MONTH", "MOVEMENTNAME",
				"NAMES", "NATIONAL", "NATURAL", "NCHAR", "NESTED", "NEW", "NEWLINE", "NEXT",
				"NO", "NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCREATEDB",
				"NOCREATEUSER", "NOCYCLE", "NODE", "NOFORCE", "NOFOUND", "NOLOGGING", "NONE", 
				"NOORDER", "NOPARALLEL", "NOT", "NOTFOUND", "NOTHING", "NOTIFY", "NOTNULL",
				"NOVALIDATE", "NOWAIT", "NULL", "NULLIF", "NULLS", "NUMBER", "NUMERIC",
				"NVARCHAR", "NVARCHAR2", "NVL", "NVL2", "OBJECT", "OF", "OFF", "OFFLINE", 
				"OFFSET", "OIDINDEX", "OIDS", "OLD", "ON", "ONLINE", "ONLY", "OPEN", 
				"OPERATOR", "OPTION", "OR", "ORDER", "ORGANIZATION", "OTHERVALUES", "OUT",
				"OUTER", "OVER", "OVERLAPS", "OWNERPACKAGE", "PARALLEL", "PARAMETERS", 
				"PARTIAL", "PARTITION", "PARTITIONS", "PASSWORD", "PCTFREE", "PCTINCREASE",
				"PCTUSED", "PCTVERSION", "PERIOD", "POLICY", "PRAGMA", "PREBUILT", "PRECEDING",
				"PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIORITY", "PRIVILEGES",
				"PROCEDURAL", "PROCEDURE", "PROTECTED", "PUBLICQUERY", "QUOTARAISE", "RANGE", 
				"RAW", "READ", "READS", "REBUILD", "RECOMPILE", "RECORD", "RECORDS", "RECYCLE",
				"REDUCED", "REF", "REFERENCES", "REFERENCING", "REFRESH", "REINDEX", "RELATIVE", 
				"RENAME", "REPEATABLE",  "REPLACE", "REPLICATION", "RESOURCE", "RESTART",
				"RESTORE", "RESTRICT", "RESULT", "RETURN", "RETURNING", "REVERSE", "REVOKE",
				"REWRITE", "RIGHT", "RIGHTOF", "ROLE", "ROLLBACK", "ROLLUP", "ROVERLAPS", 
				"ROW", "ROWCOUNT", "ROWID", "ROWS", "ROWTYPE", "RULE", "RUNSAVEPOINT", "SCHEMA",
				"SCROLL", "SECOND", "SEGMENT", "SELECT", "SELF", "SEQUENCE", "SERIALIZABLE",
				"SESSION", "SET", "SETOF", "SETS", "SHARE", "SHOW", "SHUTDOWN", "SIBLINGS",
				"SIZE", "SLOW", "SNAPSHOT", "SOME", "SPATIAL", "SPLIT", "SSO", "STANDBY",
				"START", "STATEMENT", "STATIC", "STATISTICS", "STEP", "STOP", "STORAGE", "STORE",
				"STREAM", "SUBPARTITIONSUBPARTITIONS", "SUBTYPE", "SUCCESSFUL", "SYNONYM", 
				"SYSTEMTABLE", "TABLESPACE", "TEMP", "TEMPLATE", "TEMPORARY", 
				"TERMINATED", "THAN", "THEN", "THROW", "TIME", "TIMESTAMP", "TO", "TOP",
				"TOPOVERLAPS", "TOUCHES", "TRACE", "TRAILING", "TRAN", "TRANSACTION",
				"TRIGGER", "TRUE", "TRUNCATE", "TRUSTED", "TRY", "TYPEUNBOUNDED", "UNDER",
				"UNDO", "UNIFORM", "UNION", "UNIQUE", "UNLIMITED", "UNLISTEN", "UNLOCK",
				"UNPROTECTED", "UNTIL", "UOVERLAPS", "UPDATE", "USE", "USER", "USINGVACUUM",
				"VALID", "VALIDATE", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARRAY",
				"VARYING", "VERBOSE", "VERSION", "VIEW", "VOCABLEWAIT", "WHEN", "WHENEVER",
				"WHERE", "WHILE", "WITH", "WITHIN", "WITHOUT", "WORK", "WRITE", "XML", "YEAR", "ZONE"
			};
		}
		
		@Override
		public String getExtraOptionsHelpText(){
			return "";
		}
		
		//@Override
		public String[] getUsedLibraries(){
			return new String[]{"cloudjdbcV1002.jar"};
		}
		
		@Override
		public String quoteSQLString(String string){
			string = string.replaceAll("'", "\\\\'");
			string = string.replaceAll("\\n", "\\\\n");
			string = string.replaceAll("\\r",  "\\\\r");
			return "'" + string +"'";
		}
		
		@Override
		public boolean releaseSavepoint(){
			return false;
		}
		
		@Override
		public boolean supportsErrorHandlingOnBatchUpdates(){
			return true;
		}
		
		@Override
		public boolean supportsRepository(){
			return true;
		}
		
		// need or not?
//		private static final int VARCHAR_LIMIT = 65_535;
//		@Override
//		  public int getMaxVARCHARLength() {
//		    return VARCHAR_LIMIT;
//		  }
//
//		 @Override
//		 public int getMaxTextFieldLength() {
//		   return Integer.MAX_VALUE;
//		 }
//		 
//		 @Override
//			public String getSQLLockTables(String[] tableNames){
//				String sql = "LOCK TABLES ";
//				for(int i = 0; i < tableNames.length; i++){
//					if( i > 0){
//						sql += ", ";
//					}
//					sql += tableNames[i] + "WRITE";
//				}
//				sql += ";" + Const.CR;
//				return sql;
//			}
//			
//			@Override
//			public String getSQLUnlockTables(String[] tableNames){
//				return "UNLOCK TABLES";
//			}
//			
//			@Override
//			public boolean needsToLockAllTables(){
//				return true;
//			}
//			
//			@Override
//			public boolean supportsPreparedStatementMetadataRetrieval() {
//			   return false;
//			}
}
