package org.pentaho.di.steps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import com.xugu.stream.XuguStreamOutput;

/**
 * Step类任务：实现步骤的具体任务
 * 1.初始化步骤
 * 2.执行步骤逻辑
 * 3.销毁步骤
 * @author xugu-publish
 * @since 1.8
 */
public class XuguStreamInputStep extends BaseStep implements StepInterface{
	private static final Class<?> PKG = XuguStreamInputStepMeta.class;
	private Connection conn = null;
	private Statement stmt = null;
	private String schemaName = "";
	private String tableName = "";
	private int colSum = 0;
	private Vector<Vector<Object>> vrs = new Vector<Vector<Object>>();
	/**
	 * 为流式接口构造字段信息
	 */
	private Vector<Vector<Object>> colSource = new Vector<Vector<Object>>();
	
	public XuguStreamInputStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	/**
	 * 初始化
	 */
	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		XuguStreamInputStepMeta meta = (XuguStreamInputStepMeta)smi;
		XuguStreamInputStepData data = (XuguStreamInputStepData)sdi;
		if(!super.init(meta, data)) {
			return false;
		}
		return true;
	}
	
	/**
	 * 处理行数据（被spoon循环调用）
	 */
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException{
		XuguStreamInputStepMeta meta = (XuguStreamInputStepMeta)smi;
		
		// 构造XuguStreamOutput对象用于进行流式操作
		XuguStreamOutput xs = new XuguStreamOutput();
		
		DatabaseMeta dbMeta = null;
		// 获取当前输入行数据
		Object[] r = getRow();
		
		//没有待处理的行则结束 并停止对于processRow的调用 
		if(r==null) {
			setOutputDone();
			log.logBasic("RRRRelease");
			try {
				//若语句非空证明有插入操作 调用函数执行并刷写
				if(stmt!=null) {
					String exeFlag = xs.doExecute(vrs, stmt);
					String flushFlag = xs.doFlush(stmt);
					log.logBasic(exeFlag+flushFlag);
					stmt.close();
				}
				//最后关闭数据库连接
				if(conn!=null) {
					conn.close();
				}
			}catch(SQLException e) {
				e.printStackTrace();
			}
			return false;
		}
		
		
		// first是接口中定义的标志 用于在循环开始的第一次进行一些处理
		if(first) {
			first = false;
			// 元信息的构造只能进行一次 初始化之后就不再改变
			// 获取列数
			colSum=((RowMetaInterface) getInputRowMeta()).size();
			// 获取输入行结构 
			List<ValueMetaInterface> vlist = ((RowMetaInterface) getInputRowMeta()).getValueMetaList();
			for(ValueMetaInterface v:vlist) {
				Vector<Object> temp = new Vector<Object>();
				//占位
				temp.add("0");
				//字段类型
				temp.add(v.getTypeDesc());
				//将字段加入字段数组
				colSource.add(temp);
			}
			// 建立数据库连接用于输出
			try {
				Class.forName("com.xugu.cloudjdbc.Driver");
				//point 从前台获取相关参数 数据库连接 表名
				dbMeta = meta.getDatabaseMeta();
				log.logBasic("DBConn ??? "+meta.getDatabaseMeta());
				log.logBasic("DBConn ??? "+dbMeta.getURL());
				log.logBasic("Conn?? "+dbMeta.getURL());
				String url = dbMeta.getURL();
				String userName = dbMeta.getUsername();
				String pwd = dbMeta.getPassword();
				conn = DriverManager.getConnection(url,userName,pwd);
				//conn = DriverManager.getConnection("jdbc:xugu://192.168.2.77:5138/testdb","SYSDBA","SYSDBA");
				stmt = conn.createStatement();
				
				// 获取目标表名
				tableName = meta.getTableName();
				schemaName = meta.getSchemaName();
				String targetName = (schemaName!=null && schemaName.length()!=0)?(schemaName+"."+tableName):(tableName);
				log.logBasic("TTTTTTTTarget "+targetName);
				//调用流式接口初始化函数进行准备工作
				String preFlag = xs.doInit(stmt, targetName);
				log.logBasic(preFlag);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// 构造输入行数据
		Vector<Object> row = new Vector<Object>();
		for(int i=0; i<colSum; i++) {
			row.add(r[i]);
		}
		
		// 调用xugu-stream jar包
		// 调用xugu-stream依赖包构造一行流数据
		Vector<Object> rowVector = xs.getXuguOutputStreamByOne(stmt, row, colSource);
		// 将行添加到行数组中等待最后执行和刷写
		vrs.add(rowVector);
		// 进行常规日志操作
		if(checkFeedback( getLinesRead())) {
			// Some basic logging
			logBasic( BaseMessages.getString( PKG, "XuguStreamInputStep.Linenr", getLinesRead() ) ); 
		}
		return true;
	}
	
	/**
	 * 销毁
	 */
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		XuguStreamInputStepMeta meta = (XuguStreamInputStepMeta)smi;
		XuguStreamInputStepData data = (XuguStreamInputStepData)sdi;
		super.dispose(meta, data);
	}
}
