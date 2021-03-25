package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn extends Column {

	/**
	 * 使用一个boolean值 对列进行初始化 这里除了指定类型外 还声明了boolean的长度为一个byte
	 * @param bool
	 */
	public BoolColumn(Boolean bool) {
		super(bool, Column.Type.BOOL, 1);
	}

	public BoolColumn(final String data) {
		this(true);
		this.validate(data);
		if (null == data) {
			this.setRawData(null);
			this.setByteSize(0);
		} else {
			this.setRawData(Boolean.valueOf(data));
			this.setByteSize(1);
		}
		return;
	}

	public BoolColumn() {
		super(null, Column.Type.BOOL, 1);
	}

	@Override
	public Boolean asBoolean() {
		if (null == super.getRawData()) {
			return null;
		}

		return (Boolean) super.getRawData();
	}

	@Override
	public Long asLong() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBoolean() ? 1L : 0L;
	}

	@Override
	public Double asDouble() {
		if (null == this.getRawData()) {
			return null;
		}

		return this.asBoolean() ? 1.0d : 0.0d;
	}

	@Override
	public String asString() {
		if (null == super.getRawData()) {
			return null;
		}

		return this.asBoolean() ? "true" : "false";
	}

	@Override
	public BigInteger asBigInteger() {
		if (null == this.getRawData()) {
			return null;
		}

		return BigInteger.valueOf(this.asLong());
	}

	@Override
	public BigDecimal asBigDecimal() {
		if (null == this.getRawData()) {
			return null;
		}

		return BigDecimal.valueOf(this.asLong());
	}

	@Override
	public Date asDate() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bool类型不能转为Date .");
	}

	@Override
	public byte[] asBytes() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Boolean类型不能转为Bytes .");
	}

	/**
	 * 确保string可以被转换成boolean类型
	 * @param data
	 */
	private void validate(final String data) {
		if (null == data) {
			return;
		}

		if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) {
			return;
		}

		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT,
				String.format("String[%s]不能转为Bool .", data));
	}
}
