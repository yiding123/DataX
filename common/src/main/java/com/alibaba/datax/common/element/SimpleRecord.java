package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

/**
 * 用于简易record处理，无需处理column细节，直接给record赋值
 * 例如同构数据源间copy数据
 * @author yiding  2020/12/28
 */
public class SimpleRecord implements Record {

	private List<SimpleColumn> columns = null;

	public SimpleRecord(List<SimpleColumn> columns) {
		this.columns = columns;
	}

	/**
	 *  根据column名称获取
	 * @param searchColName
	 * @return
	 */
	public Column getColumn(String searchColName){
		if(searchColName == null || searchColName.trim().length() == 0
				|| columns == null || columns.size() == 0){
			return null;
		}
		for(SimpleColumn column : columns){
			String colName = column.getName();
			if(searchColName.equalsIgnoreCase(colName)){
				return column;
			}
		}
		return null;
	}

	public void setColumn(SimpleColumn finalColumn){
		if(finalColumn == null || finalColumn.getName() == null || columns == null || columns.size() == 0){
			return;
		}
		for(int i = 0 ; i < columns.size() ;i++){
			SimpleColumn column = columns.get(i);
			if(finalColumn.getName().equalsIgnoreCase(column.getName())){
				columns.set(i, finalColumn);
				break;
			}
		}
	}

	/**
	 * 获取当前行所有列及值
	 * @return
	 */
	public List<SimpleColumn> getColumns(){
		return this.columns;
	}

	@Override
	public void addColumn(Column column) {
		if(!(column instanceof SimpleColumn)){
			throw DataXException.asDataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("参数为SimpleColumn，而传入类型为[\"%s\"]", column.getClass().getName()));
		}
		columns.add((SimpleColumn)column);
	}

	@Override
	public Column getColumn(int i) {
		if (i < 0 || i >= columns.size()) {
			return null;
		}
		return columns.get(i);
	}

	@Override
	public void setColumn(int i, final Column column) {
		if (i < 0) {
			throw DataXException.asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT, "不能给index小于0的column设置值");
		}
		SimpleColumn colOrginal = this.columns.get(i);
		colOrginal.setRawData(column.getRawData());
		//this.columns.set(i, colOrginal);
	}

	@Override
	public int getColumnNumber() {
		return columns.size();
	}

	@Override
	public int getByteSize() {
		return 0;
	}

	@Override
	public int getMemorySize() {
		return 0;
	}

}
