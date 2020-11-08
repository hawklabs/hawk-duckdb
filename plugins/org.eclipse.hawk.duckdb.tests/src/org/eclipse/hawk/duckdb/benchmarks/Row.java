package org.eclipse.hawk.duckdb.benchmarks;

public class Row {
	public String indexName;
	public String keyName;
	public long keyValue;
	public long nodeId;

	public Row(String indexName, String keyName, long keyValue, long nodeId) {
		this.indexName = indexName;
		this.keyName = keyName;
		this.keyValue = keyValue;
		this.nodeId = nodeId;
	}
}