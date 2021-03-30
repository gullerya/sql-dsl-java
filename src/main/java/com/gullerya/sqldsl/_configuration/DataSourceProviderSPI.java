package com.gullerya.sql._configuration;

public interface DataSourceProviderSPI {

	default String getDBConfigLocation() {
		return DataSourceProvider.DEFAULT_CONFIG_LOCATION;
	}
}
