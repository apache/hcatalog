package org.apache.hcatalog.metadata;

public class HPartitionDoneEvent {

	private String dbName;
	
	private String tblName;
	
	private String partVals;

	private Long createTime;
	
	/**
	 * @return the createTime
	 */
	public Long getCreateTime() {
		return createTime;
	}

	HPartitionDoneEvent(String dbName, String tblName, String partSpec) {
		super();
		this.dbName = dbName;
		this.tblName = tblName;
		this.partVals = partSpec;
		this.createTime = System.currentTimeMillis();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HPartitionDoneEvent [dbName=" + dbName + ", tblName=" + tblName
				+ ", partSpec=" + partVals + ", createTime=" + createTime + "]";
	}

	public HPartitionDoneEvent() {}

	/**
	 * @param dbName the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * @param tblName the tblName to set
	 */
	public void setTblName(String tblName) {
		this.tblName = tblName;
	}

	/**
	 * @param partSpec the partSpec to set
	 */
	public void setPartSpec(String partSpec) {
		this.partVals = partSpec;
	}

	/**
	 * @param createTime the createTime to set
	 */
	public void setCreateTime(Long createTime) {
		this.createTime = createTime;
	}
}
