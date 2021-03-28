package com.gullerya.typedsql.configuration;

import javax.sql.DataSource;

class DataSourceDetailsImpl implements DataSourceDetails {
	private final String dbType;
	private final String url;
	private final String user;
	private final String pass;
	private final DataSource dataSource;

	DataSourceDetailsImpl(String dbType, String url, String user, String pass, DataSource dataSource) {
		this.dbType = dbType;
		this.url = url;
		this.user = user;
		this.pass = pass;
		this.dataSource = dataSource;
	}

	@Override
	public String getDBType() {
		return dbType;
	}

	@Override
	public String getUrl() {
		return url;
	}

	@Override
	public String getUser() {
		return user;
	}

	@Override
	public String getPass() {
		return pass;
	}

	@Override
	public DataSource getDataSource() {
		return dataSource;
	}
}
