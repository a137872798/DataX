package com.alibaba.datax.common.element;

/**
 * Created by jingxing on 14-8-24.
 * 在数据传输的工程中 每条数据作为一个Record 同时一个Record由多个Column组成
 */
public interface Record {

	public void addColumn(Column column);

	public void setColumn(int i, final Column column);

	public Column getColumn(int i);

	public String toString();

	public int getColumnNumber();

	public int getByteSize();

	public int getMemorySize();

}
