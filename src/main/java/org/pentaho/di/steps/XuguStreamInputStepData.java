package org.pentaho.di.steps;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * 用于存储临时数据、文件句柄等等
 * 
 * @author xugu-publish
 * @since 1.8
 *
 */
public class XuguStreamInputStepData extends BaseStepData implements StepDataInterface {
	RowMetaInterface outputRowMeta;
	int outputFieldIndex = -1;

	public XuguStreamInputStepData() {
		super();
	}

}
