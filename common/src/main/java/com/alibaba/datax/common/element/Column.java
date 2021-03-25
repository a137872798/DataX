package com.alibaba.datax.common.element;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * 在数据传输的过程中 数据被看作是一个个列
 */
public abstract class Column {

	/**
	 * 当前数据列的类型
	 */
	private Type type;

	/**
	 * 应该是数据被reader传输过来的原始值 可能需要适配成合适writer的类型
	 */
	private Object rawData;

	private int byteSize;

	public Column(final Object object, final Type type, int byteSize) {
		this.rawData = object;
		this.type = type;
		this.byteSize = byteSize;
	}

	public Object getRawData() {
		return this.rawData;
	}

	public Type getType() {
		return this.type;
	}

	public int getByteSize() {
		return this.byteSize;
	}

	protected void setType(Type type) {
		this.type = type;
	}

	protected void setRawData(Object rawData) {
		this.rawData = rawData;
	}

	protected void setByteSize(int byteSize) {
		this.byteSize = byteSize;
	}

	public abstract Long asLong();

	public abstract Double asDouble();

	public abstract String asString();

	public abstract Date asDate();

	public abstract byte[] asBytes();

	public abstract Boolean asBoolean();

	public abstract BigDecimal asBigDecimal();

	public abstract BigInteger asBigInteger();

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

	public enum Type {
		BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES
	}
}
