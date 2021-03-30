package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 用于简易处理column
 * 目前仅适用于 mongodb，其他未测试
 * @author yiding  2020/12/28
 */
public class SimpleColumn extends Column {

    public SimpleColumn(String name , Object value){
        this(value, Type.NULL,value.toString().length());
        this.setName(name);
    }

    public SimpleColumn(Object object, Type type, int byteSize) {
        super(object, type, byteSize);
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