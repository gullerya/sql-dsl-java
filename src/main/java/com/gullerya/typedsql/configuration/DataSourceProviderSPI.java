package com.gullerya.typedsql.configuration;

public interface DataSourceProviderSPI {

	default String getDBConfigLocation() {
		return DataSourceProvider.DEFAULT_CONFIG_LOCATION;
	}
}
