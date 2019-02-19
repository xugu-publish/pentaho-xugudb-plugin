package org.pentaho.di.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Vector;

//该注解指明该类为数据库插件类
@DatabaseMetaPlugin(
type = "Xugu",
typeDescription = "Xugu"
)

public class XuguDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface{		
	// 返回连接类型 目前暂时支持JDBC
	@Override
	public int[] getAccessTypeList(){
		return new int[]{ DatabaseMeta.TYPE_ACCESS_NATIVE};
	}

	// 返回默认端口
	@Override
	public int  getDefaultDatabasePort(){
		return 5138;
	}
	
	// 为查询限制行数
	@Override
	public String getLimitClause(int nrRows){
		return " LIMIT " + nrRows;// mysql
	}

	// 返回一个关于表的最小查询语句
	@Override
	public String getSQLQueryFields(String tableName){
		return "SELECT * FROM " + tableName + " LIMIT 0";
	}
	
	// 通过查询语句判断表是否存在 若抛出异常则表不存在
	@Override
	public String getSQLTableExists(String tableName){
		return getSQLQueryFields(tableName);
	}

	// 返回一个关于列的最小查询语句 
	public String getSQLQueryColumnFields(String columnName, String tableName){
		return "SELECT " + columnName + " FROM " + tableName + " LIMIT 0";
	}
	
	// 通过查询语句判断列是否存在 若抛出异常则列不存在
	@Override
	public String getSQLColumnExists(String columnName, String tableName){
		return getSQLQueryColumnFields(columnName, tableName);
	}

	// 返回驱动类
	public String getDriverClass(){
		return "com.xugu.cloudjdbc.Driver";
	}
	
	// 返回数据库连接url
	public String getURL(String hostName, String port, String databaseName){
		Properties properties = new Properties();
		// 使用InPutStream流读取properties文件
		BufferedReader bufferedReader;
		try {
			bufferedReader = new BufferedReader(new FileReader("connectProperty.properties"));
			properties.load(bufferedReader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// 获取key对应的value值
		String char_set = properties.getProperty("char_set");
		String lock_time = properties.getProperty("lock_time");
		String lob_ret = properties.getProperty("lob_ret");
		String return_rowid = properties.getProperty("return_rowid");
		String auto_commit = properties.getProperty("auto_commit");
		String conn_type = properties.getProperty("conn_type");
		String ips = properties.getProperty("ips");
		String sql_cursor = properties.getProperty("sql_cursor");
		// 构建连接字符串
		String con_str = "jdbc:xugu://" + hostName + ":" + port + "/" +databaseName;
		Vector<String> pro_str = new Vector<String>();
		if(char_set!=null && char_set.length()>0) {
			pro_str.add("char_set="+char_set);
		}
		if(lock_time!=null && lock_time.length()>0) {
			pro_str.add("lock_time="+lock_time);
		}
		if(lob_ret!=null && lob_ret.length()>0) {
			pro_str.add("lob_ret="+lob_ret);		
		}
		if(return_rowid!=null && return_rowid.length()>0) {
			pro_str.add("return_rowid="+return_rowid);
		}
		if(auto_commit!=null && auto_commit.length()>0) {
			pro_str.add("auto_commit="+auto_commit);
		}
		if(conn_type!=null && conn_type.length()>0) {
			pro_str.add("conn_type="+conn_type);
		}
		if(ips!=null && ips.length()>0) {
			pro_str.add("ips="+ips);
		}
		if(sql_cursor!=null && sql_cursor.length()>0) {
			pro_str.add("sql_cursor="+sql_cursor);
		}
		if(pro_str.size()!=0) {
			con_str+="?";
		}
		for(int i=0; i<pro_str.size(); i++) {
			if(i!=0) {
				con_str += "&";
			}
			con_str += pro_str.get(i);
		}
		return con_str;
	}

	//是否支持同义词
	@Override
	public boolean supportsSynonyms(){
		return true;
	}

	// 返回字段定义 用于组装sql语句
	public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldName, boolean add_cr){
		String retval = "";
		//获取字段信息
		String fieldName = v.getName();
		//clob类型长度处理 长度一致则不需要处理
		//			if(v.getLength() == DatabaseMeta.CLOB_LENGTH) {
		//				v.setLength(getMaxTextFieldLength());
		//			}
		//字段长度
		int length = v.getLength();
		//字段精度
		int precision = v.getPrecision();
		//字段类型
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
							// 10-18位整数 设为BIGINT
							if(length<19) {
								retval += "BIGINT";
							}
							// 19位及以上设为NUMERIC
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
			default:
				break;
		}
		
		if(add_cr) {
			retval += Const.CR;
		}
		
		return retval;
	}

	// 构造增加列的Sql语句
	public String getAddColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon){
		String s =  "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
		System.out.println("ADD..."+s);
		return "ALTER TABLE " + tableName + " ADD COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
	}

	// 构造修改列的Sql语句
	public String getModifyColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon){		
		String s = "ALTER TABLE " + tableName +" ALTER COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
		System.out.println("Modify..."+s);
		return "ALTER TABLE " + tableName +" ALTER COLUMN " + getFieldDefinition( v, tk, pk, use_autoinc, true, false);
	}

	// 构造删除列的Sql语句
	@Override
	public String getDropColumnStatement(String tableName, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
		String s = "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
		System.out.println("Drop..."+s);
		return "ALTER TABLE " + tableName + " DROP COLUMN " + v.getName();
	}

	// 获取保留字
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

	// 获取在线帮助文档地址
	@Override
	public String getExtraOptionsHelpText(){
		return "";
	}

	// 获取所需的jar包
	public String[] getUsedLibraries(){
		return new String[]{"cloudjdbcV1002.jar"};
	}

	// 格式化Sql语句
	@Override
	public String quoteSQLString(String string){
		string = string.replaceAll("'", "\\\\'");
		string = string.replaceAll("\\n", "\\\\n");
		string = string.replaceAll("\\r",  "\\\\r");
		return "'" + string +"'";
	}

	// 可否释放SavePoint
	@Override
	public boolean releaseSavepoint(){
		return false;
	}
	
	// 是否支持在批处理过程中进行错误处理
	@Override
	public boolean supportsErrorHandlingOnBatchUpdates(){
		return true;
	}

	// 是否支持kettel本地仓库
	@Override
	public boolean supportsRepository(){
		return true;
	}
}
