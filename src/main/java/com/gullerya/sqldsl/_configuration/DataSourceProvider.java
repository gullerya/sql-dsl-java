package com.gullerya.sql._configuration;

import java.util.HashMap;
import java.util.Map;

public abstract class DataSourceProvider {
	public static final String DEFAULT_DATASOURCE_KEY = "default";
	public static final String CONFIG_LOCATION_KEY = "db.config.location";
	public static final String DEFAULT_CONFIG_LOCATION = "db.properties";
	public static final String CONFIG_FROM_SYSENV = "environment";
	public static final String CONFIG_FROM_SYSPROPS = "sysprops";

	/**
	 * returns "default" DataSource
	 * - this method returns the same as invoking getDataSource method with parameter "default"
	 *
	 * @return data source details or NULL
	 */
	abstract public DataSourceDetails getDataSourceDetails();

	/**
	 * returns DataSource details associated with the specified key
	 * - if the key "default" specified, result should be same as if invoking parameter-less {@link DataSourceProvider#getDataSourceDetails()}
	 *
	 * @param key data source key
	 * @return data source details or NULL
	 */
	abstract public DataSourceDetails getDataSourceDetails(String key);

	public static DataSourceProvider getInstance() {
		return DataSourceProviders.getDataSourceProvider();
	}

	public static DataSourceProvider getInstance(DataSourceProviderSPI configurer) {
		return DataSourceProviders.getDataSourceProvider(configurer);
	}

	private static final class DataSourceProviders {
		private static final Map<DataSourceProviderSPI, DataSourceProvider> dsProviders = new HashMap<>();
		private static final Object dsInitLock = new Object();
		private static final DataSourceProviderSPI defaultConfigurer = new DataSourceProviderSPI() {
		};

		private DataSourceProviders() {
		}

		private static DataSourceProvider getDataSourceProvider() {
			return getDataSourceProvider(defaultConfigurer);
		}

		private static DataSourceProvider getDataSourceProvider(DataSourceProviderSPI dspConfigurer) {
			if (dspConfigurer == null) {
				throw new IllegalArgumentException("data source provider configurer MUST NOT be NULL");
			}

			DataSourceProvider result = dsProviders.get(dspConfigurer);
//			if (result == null) {
//				synchronized (dsInitLock) {
//					result = dsProviders.get(dspConfigurer);
//					if (result == null) {
//						result = new DataSourceProviderImpl(dspConfigurer);
//						dsProviders.put(dspConfigurer, result);
//					}
//				}
//			}
			return result;
		}
	}
}
