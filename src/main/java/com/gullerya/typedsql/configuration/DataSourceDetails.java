package com.gullerya.typedsql.configuration;

import javax.sql.DataSource;

public interface DataSourceDetails {

	String getDBType();

	String getUrl();

	String getUser();

	String getPass();

	DataSource getDataSource();
}
