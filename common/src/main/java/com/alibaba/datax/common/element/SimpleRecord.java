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
 */
public class SimpleRecord implements Record {

	private List<SimpleColumn> columns = null;

	public SimpleRecord(List<SimpleColumn> columns) {
		this.columns = columns;
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

	public static class SimpleColumn extends Column {

		/**
		 * 列名
		 */
		private	String name;

		public SimpleColumn(String name , Object value){
			this(value, Type.NULL,value.toString().length());
			this.name = name;
		}

		public SimpleColumn(Object object, Type type, int byteSize) {
			super(object, type, byteSize);
		}

		public String getName() {
			return name;
		}

		@Override
		public String asString() {
			if (null == this.getRawData()) {
				return null;
			}
			return this.getRawData().toString();
		}

		private void validateDoubleSpecific(final String data) {
			if ("NaN".equals(data) || "Infinity".equals(data)
					|| "-Infinity".equals(data)) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT,
						String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型 .", data));
			}

			return;
		}

		@Override
		public BigInteger asBigInteger() {
			if (null == this.getRawData()) {
				return null;
			}

			this.validateDoubleSpecific(this.getRawData().toString());

			try {
				return this.asBigDecimal().toBigInteger();
			} catch (Exception e) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
								"String[\"%s\"]不能转为BigInteger .", this.asString()));
			}
		}

		@Override
		public Long asLong() {
			if (null == this.getRawData()) {
				return null;
			}

			this.validateDoubleSpecific(this.getRawData().toString());

			try {
				BigInteger integer = this.asBigInteger();
				OverFlowUtil.validateLongNotOverFlow(integer);
				return integer.longValue();
			} catch (Exception e) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT,
						String.format("String[\"%s\"]不能转为Long .", this.asString()));
			}
		}

		@Override
		public BigDecimal asBigDecimal() {
			if (null == this.getRawData()) {
				return null;
			}

			this.validateDoubleSpecific(this.getRawData().toString());

			try {
				return new BigDecimal(this.asString());
			} catch (Exception e) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
								"String [\"%s\"] 不能转为BigDecimal .", this.asString()));
			}
		}

		@Override
		public Double asDouble() {
			if (null == this.getRawData()) {
				return null;
			}

			String data = this.getRawData().toString();
			if ("NaN".equals(data)) {
				return Double.NaN;
			}

			if ("Infinity".equals(data)) {
				return Double.POSITIVE_INFINITY;
			}

			if ("-Infinity".equals(data)) {
				return Double.NEGATIVE_INFINITY;
			}

			BigDecimal decimal = this.asBigDecimal();
			OverFlowUtil.validateDoubleNotOverFlow(decimal);

			return decimal.doubleValue();
		}

		@Override
		public Boolean asBoolean() {
			if (null == this.getRawData()) {
				return null;
			}

			if ("true".equalsIgnoreCase(this.asString())) {
				return true;
			}

			if ("false".equalsIgnoreCase(this.asString())) {
				return false;
			}

			throw DataXException.asDataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("String[\"%s\"]不能转为Bool .", this.asString()));
		}

		@Override
		public Date asDate() {
			try {
				return ColumnCast.string2Date(new StringColumn(this.getRawData().toString()));
			} catch (Exception e) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT,
						String.format("String[\"%s\"]不能转为Date .", this.asString()));
			}
		}

		@Override
		public byte[] asBytes() {
			try {
				return ColumnCast.string2Bytes(new StringColumn(this.getRawData().toString()));
			} catch (Exception e) {
				throw DataXException.asDataXException(
						CommonErrorCode.CONVERT_NOT_SUPPORT,
						String.format("String[\"%s\"]不能转为Bytes .", this.asString()));
			}
		}
	}
}
