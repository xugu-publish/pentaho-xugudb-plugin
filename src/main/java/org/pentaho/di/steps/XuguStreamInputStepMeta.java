package org.pentaho.di.steps;

import java.util.List;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(id = "XuguStreamInputStep", name = "XuguStreamInputStep", description = "XuguStreamInputStep.TooltipDesc", image = "org/pentaho/di/steps/resources/demo.svg"
//		categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Transform",
//		i18nPackageName = "org.pentaho.di.steps.xugu",
//		documentationUrl = "XuguStreamInputStep.DocumentationURL",
//		casesUrl = "XuguStreamInputStep.CaseURL",
//		forumUrl = "XuguStreamInputStep.ForumURL"
)
/**
 * 提供步骤元信息以及处理串行化
 * 
 * @author xugu-publish
 * @since 1.8
 */
@InjectionSupported(localizationPrefix = "XuguStreamInputStepMeta.Injection.")
public class XuguStreamInputStepMeta extends BaseStepMeta implements StepMetaInterface {

	/**
	 * for i18n purposes 国际化
	 */
	private static final Class<?> PKG = XuguStreamInputStepMeta.class;
	private DatabaseMeta databaseMeta;

	@Injection(name = "CONNECTION")
	private String connection;
	private String schemaName;
	private String tableName;

	/**
	 * 调用父类构造函数以正确初始化
	 */
	public XuguStreamInputStepMeta() {
		super();
	}

	/**
	 * 获取所需的jar包
	 */
	@Override
	public String[] getUsedLibraries() {
		return new String[] { "xugu-stream-plugin.jar" };
	}

	/**
	 * 被spoon调用来为本步骤创建一个对话框
	 * 
	 * @param shell
	 * @param meta
	 * @param transMeta
	 * @param name
	 * @return
	 */
	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new XuguStreamInputStepDialog(shell, meta, transMeta, name);
	}

	/**
	 * 被pdi调用以创建一个新的step实例
	 */
	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans disp) {
		return new XuguStreamInputStep(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	/**
	 * 被pdi调用以创建一个新的step data实例
	 */
	@Override
	public StepDataInterface getStepData() {
		return new XuguStreamInputStepData();
	}

	/**
	 * 设置元数据默认值
	 */
	@Override
	public void setDefault() {
	}

	public DatabaseMeta getDatabaseMeta() {
		return databaseMeta;
	}

	public void setDatabaseMeta(DatabaseMeta database) {
		this.databaseMeta = database;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * 被spoon调用用于用于深复制
	 */
	@Override
	public Object clone() {
		Object retval = super.clone();
		return retval;
	}

	/**
	 * 被spoon调用用以序列化配置信息（序列化）
	 */
	@Override
	public String getXML() throws KettleValueException {
		StringBuilder xml = new StringBuilder();
		// 仅处理一个字段
		xml.append(XMLHandler.addTagValue("connection", connection));
		xml.append("    " + XMLHandler.addTagValue("connection", databaseMeta == null ? "" : databaseMeta.getName()));
		xml.append("    " + XMLHandler.addTagValue("schema", schemaName));
		xml.append("    " + XMLHandler.addTagValue("table", tableName));
		return xml.toString();
	}

	/**
	 * 被pdi调用用以加载xml文件中的配置信息（反序列化）
	 */
	@Override
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		try {
			// point 重要 获取前台输入 根据标签中的值进行设置
			String con = XMLHandler.getTagValue(stepnode, "connection");
			databaseMeta = DatabaseMeta.findDatabase(databases, con);
			schemaName = XMLHandler.getTagValue(stepnode, "schema");
			tableName = XMLHandler.getTagValue(stepnode, "table");
		} catch (Exception e) {
			throw new KettleXMLException("plugin unable to read step info from XML node", e);
		}
	}

	/**
	 * 被spoon调用用以将该step的配置信息序列化并保存至仓库（序列化）
	 */
	@Override
	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId idTransformation, ObjectId idStep)
			throws KettleException {
		try {
			rep.saveStepAttribute(idTransformation, idStep, "connection", connection);
			rep.saveStepAttribute(idTransformation, idStep, "schema", schemaName);
			rep.saveStepAttribute(idTransformation, idStep, "table", tableName);
		} catch (Exception e) {
			throw new KettleException("Unable to save step into repository:" + idStep, e);
		}
	}

	/**
	 * 被pdi调用用于从仓库中读取配置信息（反序列化）
	 */
	@Override
	public void readRep(Repository rep, IMetaStore metaStore, ObjectId idStep, List<DatabaseMeta> databases)
			throws KettleException {
		try {
			connection = rep.getStepAttributeString(idStep, "connection");
			schemaName = rep.getStepAttributeString(idStep, "schema");
			tableName = rep.getStepAttributeString(idStep, "table");
		} catch (Exception e) {
			throw new KettleException("Unable to load step from repository", e);
		}
	}

	/**
	 * 当用户在spoon中点击验证时调用 用以检查转换的是否正常（校验元数据是否正确）
	 * 
	 * @param remarks
	 * @param transMeta
	 * @param stepMeta
	 * @param prev
	 * @param input
	 * @param output
	 * @param info
	 * @param space
	 * @param repository
	 * @param metaStore
	 */
	@Override
	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
			String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
			IMetaStore metaStore) {
		CheckResult cr;
		// See if there are input streams leading to this step!
		if (input != null && input.length > 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
					BaseMessages.getString(PKG, "Demo.CheckResult.ReceivingRows.OK"), stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
					BaseMessages.getString(PKG, "Demo.CheckResult.ReceivingRows.ERROR"), stepMeta);
			remarks.add(cr);
		}
	}

}
