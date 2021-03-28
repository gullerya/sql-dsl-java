//package com.gullerya.typedsql.configuration;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//class DataSourceProviderImpl extends DataSourceProvider {
//	private final static Logger logger = LoggerFactory.getLogger(DataSourceProviderImpl.class);
//	private final Configurer configurer;
//
//	DataSourceProviderImpl(DataSourceProviderSPI providerSPI) {
//		configurer = new Configurer(providerSPI.getDBConfigLocation());
//	}
//
//	@Override
//	public DataSourceDetails getDataSourceDetails() {
//		return getDataSourceDetails(DEFAULT_DATASOURCE_KEY);
//	}
//
//	@Override
//	public DataSourceDetails getDataSourceDetails(String key) {
//		if (key == null || key.isEmpty()) {
//			throw new IllegalArgumentException("key MUST NOT be NULL nor EMPTY");
//		}
//		return configurer.getDataSourceByKey(key);
//	}
//}
