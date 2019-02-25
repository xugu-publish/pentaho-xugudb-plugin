package org.pentaho.di.steps;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;

/**
 * 
 * @author xugu-publish
 * @since 1.8
 */
public class XuguStreamInputStepDialog extends BaseStepDialog implements StepDialogInterface {
	private static Class<?> PKG = XuguStreamInputStepMeta.class;
	private XuguStreamInputStepMeta meta;

	private CCombo wConnection;
	private Label wlSchema;
	private TextVar wSchema;
	private FormData fdlSchema, fdSchema;
	private FormData fdbSchema;
	private Button wbSchema;

	private Label wlTable;
	private Button wbTable;
	private TextVar wTable;
	private FormData fdlTable, fdbTable, fdTable;

	/**
	 * List of ColumnInfo that should have the field names of the selected database
	 * table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

	public XuguStreamInputStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
		meta = (XuguStreamInputStepMeta) in;
	}

	/**
	 * 被spoon调用以打开窗口 当用户取消时，元数据必须不更新，返回空 当用户确认时，方法会将step名字返回
	 */
	@Override
	public String open() {

		Shell parent = getParent();
		Display display = parent.getDisplay();

		// SWT准备工作
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, meta);

		// 标志，保存现有元数据，当用户取消时，将重新获取保存的值
		changed = meta.hasChanged();

		/**
		 * ModifyListener 监听所有控制操作来设置是否有改动
		 */
		ModifyListener lsMod = new ModifyListener() {
			@Override
			public void modifyText(ModifyEvent e) {
				meta.setChanged();
			}
		};
		ModifyListener lsTableMod = new ModifyListener() {
			@Override
			public void modifyText(ModifyEvent arg0) {
				meta.setChanged();
				// setTableFieldCombo();
			}
		};

		// 界面代码
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;
		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "Xugu.Shell.Title"));
		int middle = props.getMiddlePct();
		int margin = Const.FORM_MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);

		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// Connection line
		wConnection = addConnectionLine(shell, wStepname, middle, margin);
		if (meta.getDatabaseMeta() == null && transMeta.nrDatabases() == 1) {
			wConnection.select(0);
		}
		wConnection.addModifyListener(lsMod);

		// Schema line...
		wlSchema = new Label(shell, SWT.RIGHT);
		wlSchema.setText(BaseMessages.getString(PKG, "Xugu.TargetSchema.Label"));
		props.setLook(wlSchema);
		fdlSchema = new FormData();
		fdlSchema.left = new FormAttachment(0, 0);
		fdlSchema.right = new FormAttachment(middle, -margin);
		fdlSchema.top = new FormAttachment(wConnection, margin * 2);
		wlSchema.setLayoutData(fdlSchema);

		wbSchema = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbSchema);
		wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		fdbSchema = new FormData();
		fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
		fdbSchema.right = new FormAttachment(100, 0);
		wbSchema.setLayoutData(fdbSchema);

		wbSchema.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				getSchemaNames();
			}
		});

		wSchema = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wSchema);
		wSchema.addModifyListener(lsTableMod);
		fdSchema = new FormData();
		fdSchema.left = new FormAttachment(middle, 0);
		fdSchema.top = new FormAttachment(wConnection, margin * 2);
		fdSchema.right = new FormAttachment(wbSchema, -margin);
		wSchema.setLayoutData(fdSchema);

		// Table line...
		wlTable = new Label(shell, SWT.RIGHT);
		wlTable.setText(BaseMessages.getString(PKG, "Xugu.TargetTable.Label"));
		props.setLook(wlTable);
		fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wbSchema, margin);
		wlTable.setLayoutData(fdlTable);

		wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
		props.setLook(wbTable);
		wbTable.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wbSchema, margin);
		wbTable.setLayoutData(fdbTable);

		wbTable.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent e) {
				getTableName();
			}
		});

		wTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wTable);
		wTable.addModifyListener(lsTableMod);
		fdTable = new FormData();
		fdTable.top = new FormAttachment(wbSchema, margin);
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.right = new FormAttachment(wbTable, -margin);
		wTable.setLayoutData(fdTable);

		// OK and cancel buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
		setButtonPositions(new Button[] { wOK, wCancel }, margin, wTable);

		// Add listeners for cancel and OK
		lsCancel = new Listener() {
			@Override
			public void handleEvent(Event e) {
				cancel();
			}
		};
		lsOK = new Listener() {
			@Override
			public void handleEvent(Event e) {
				ok();
			}
		};
		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		// default listener (for hitting "enter")
		lsDef = new SelectionAdapter() {
			@Override
			public void widgetDefaultSelected(SelectionEvent e) {
				ok();
			}
		};
		wStepname.addSelectionListener(lsDef);

		// Detect X or ALT-F4 or something that kills this window and cancel the dialog
		// properly
		shell.addShellListener(new ShellAdapter() {
			@Override
			public void shellClosed(ShellEvent e) {
				cancel();
			}
		});

		getData();
		// Set/Restore the dialog size based on last position on screen
		// The setSize() method is inherited from BaseStepDialog
		setSize();

		// populate the dialog with the values from the meta object
		populateDialog();

		// restore the changed flag to original value, as the modify listeners fire
		// during dialog population
		meta.setChanged(changed);

		// open dialog and enter event loop
		shell.open();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		// at this point the dialog has closed, so either ok() or cancel() have been
		// executed
		// The "stepname" variable is inherited from BaseStepDialog
		return stepname;
	}

	private void populateDialog() {
		wStepname.selectAll();
	}

	/**
	 * 当用户取消时被spoon调用
	 */
	private void cancel() {
		stepname = null;
		// 将changed标志重新保存至元数据
		meta.setChanged(changed);
		dispose();
	}

	/**
	 * 当用户确认时被spoon调用 将界面上的值设入meta中
	 */
	private void ok() {
		getInfo(meta);
		// stepname = wStepname.getText();
		// meta.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
		try {
			log.logBasic("CCCCCChanged! " + " " + meta.getDatabaseMeta() + " " + meta.getDatabaseMeta().getURL());
		} catch (KettleException e) {
			e.printStackTrace();
		}
		dispose();
	}

	private void getSchemaNames() {
		DatabaseMeta databaseMeta = transMeta.findDatabase(wConnection.getText());
		if (databaseMeta != null) {
			Database database = new Database(loggingObject, databaseMeta);
			try {
				database.connect();
				String[] schemas = database.getSchemas();

				if (null != schemas && schemas.length > 0) {
					schemas = Const.sortStrings(schemas);
					EnterSelectionDialog dialog = new EnterSelectionDialog(shell, schemas,
							BaseMessages.getString(PKG, "TableOutputDialog.AvailableSchemas.Title",
									wConnection.getText()),
							BaseMessages.getString(PKG, "TableOutputDialog.AvailableSchemas.Message",
									wConnection.getText()));
					String d = dialog.open();
					if (d != null) {
						wSchema.setText(Const.NVL(d, ""));
						setTableFieldCombo();
					}
				} else {
					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
					mb.setMessage(BaseMessages.getString(PKG, "TableOutputDialog.NoSchema.Error"));
					mb.setText(BaseMessages.getString(PKG, "TableOutputDialog.GetSchemas.Error"));
					mb.open();
				}
			} catch (Exception e) {
				new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
						BaseMessages.getString(PKG, "TableOutputDialog.ErrorGettingSchemas"), e);
			} finally {
				database.disconnect();
			}
		}
	}

	private void getTableName() {
		// New class: SelectTableDialog
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0) {
			DatabaseMeta inf = transMeta.getDatabase(connr);

			if (log.isDebug()) {
				logDebug(BaseMessages.getString(PKG, "TableOutputDialog.Log.LookingAtConnection", inf.toString()));
			}

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
			std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
			if (std.open()) {
				wSchema.setText(Const.NVL(std.getSchemaName(), ""));
				wTable.setText(Const.NVL(std.getTableName(), ""));
				setTableFieldCombo();
			}
		} else {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "TableOutputDialog.ConnectionError2.DialogMessage"));
			mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
			mb.open();
		}

	}

	private void setTableFieldCombo() {
		Runnable fieldLoader = new Runnable() {
			@Override
			public void run() {
				if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
					final String tableName = wTable.getText(), connectionName = wConnection.getText(),
							schemaName = wSchema.getText();

					// clear
					for (ColumnInfo colInfo : tableFieldColumns) {
						colInfo.setComboValues(new String[] {});
					}
					if (!Utils.isEmpty(tableName)) {
						DatabaseMeta ci = transMeta.findDatabase(connectionName);
						if (ci != null) {
							Database db = new Database(loggingObject, ci);
							try {
								db.connect();

								RowMetaInterface r = db.getTableFieldsMeta(transMeta.environmentSubstitute(schemaName),
										transMeta.environmentSubstitute(tableName));
								if (null != r) {
									String[] fieldNames = r.getFieldNames();
									if (null != fieldNames) {
										for (ColumnInfo colInfo : tableFieldColumns) {
											colInfo.setComboValues(fieldNames);
										}
									}
								}
							} catch (Exception e) {
								for (ColumnInfo colInfo : tableFieldColumns) {
									colInfo.setComboValues(new String[] {});
								}
								// ignore any errors here. drop downs will not be
								// filled, but no problem for the user
							} finally {
								try {
									if (db != null) {
										db.disconnect();
									}
								} catch (Exception ignored) {
									// ignore any errors here. Nothing we can do if
									// connection fails to close properly
									db = null;
								}
							}
						}
					}
				}
			}
		};
		shell.getDisplay().asyncExec(fieldLoader);
	}

	public void getData() {
		if (meta.getSchemaName() != null) {
			wSchema.setText(meta.getSchemaName());
		}
		if (meta.getTableName() != null) {
			wTable.setText(meta.getTableName());
		}
		if (meta.getDatabaseMeta() != null) {
			wConnection.setText(meta.getDatabaseMeta().getName());
		}
		wStepname.selectAll();
		wStepname.setFocus();
	}

	private void getInfo(XuguStreamInputStepMeta info) {
		info.setSchemaName(wSchema.getText());
		info.setTableName(wTable.getText());
		info.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()));
	}
}
