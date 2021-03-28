package com.gullerya.typedsql.configuration;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class Configurer {
	private final static Logger logger = LoggerFactory.getLogger(Configurer.class);

	//  keys for System properties (JVM options) and property files
	private final String DB_URL = "db.url";
	private final String DB_TYPE = "db.type";
	private final String DB_USER = "db.user";
	private final String DB_PASS = "db.pass";
	private final String DB_MAX_CONN = "db.maxConn";
	private final String DB_APP_NAME = "db.appName";

	//  keys for Environment variables
	private final String ENV_DB_URL = "LESSONOMY_DB_URL";
	private final String ENV_DB_TYPE = "LESSONOMY_DB_TYPE";
	private final String ENV_DB_USER = "LESSONOMY_DB_USER";
	private final String ENV_DB_PSWD = "LESSONOMY_DB_PSWD";
	private final String ENV_DB_MAX_CONN = "LESSONOMY_DB_MAX_CONN";
	private final String ENV_DB_APP_NAME = "LESSONOMY_DB_APP_NAME";

	private final Map<String, DataSourceDetails> dataSourcesRegistry = new HashMap<>();

	Configurer(String configLocation) {
		if (configLocation == null || configLocation.isEmpty()) {
			if (System.getenv(DataSourceProvider.CONFIG_LOCATION_KEY) != null) {
				configLocation = System.getenv(DataSourceProvider.CONFIG_LOCATION_KEY);
			} else if (System.getProperty(DataSourceProvider.CONFIG_LOCATION_KEY) != null) {
				configLocation = System.getProperty(DataSourceProvider.CONFIG_LOCATION_KEY);
			}
		}

		if (configLocation == null || configLocation.isEmpty()) {
			throw new IllegalArgumentException("config location parameter MUST NOT be NULL nor EMPTY");
		}

		if (DataSourceProvider.CONFIG_FROM_SYSENV.equals(configLocation)) {
			logger.info("resolving DB configuration/s from ENVIRONMENT variables...");
			initFromSysEnvironment();
		} else if (DataSourceProvider.CONFIG_FROM_SYSPROPS.equals(configLocation)) {
			logger.info("resolving DB configuration/s from JVM's SYSTEM properties...");
			initFromSysProperties();
		} else if (configLocation.endsWith(".properties")) {
			logger.info("resolving DB configuration/s from '" + configLocation + "' class path resource...");
			initFromProperties(configLocation);
		} else {
			throw new IllegalStateException("unsupported yet configuration format");
		}
		logger.info("... total DB configurations resolved: " + dataSourcesRegistry.size() + " (" + String.join(", ", dataSourcesRegistry.keySet()) + ")");
	}

	DataSourceDetails getDataSourceByKey(String key) {
		return dataSourcesRegistry.get(key);
	}

	private void initFromSysEnvironment() {
		String url = System.getenv(ENV_DB_URL);
		String type = System.getenv(ENV_DB_TYPE);
		String user = System.getenv(ENV_DB_USER);
		String pass = System.getenv(ENV_DB_PSWD);
		String maxConn = System.getenv(ENV_DB_MAX_CONN);
		String appName = System.getenv(ENV_DB_APP_NAME);

		DataSourceDetails dsd = createDataSource(url, type, user, pass, maxConn, appName);
		dataSourcesRegistry.put(DataSourceProvider.DEFAULT_DATASOURCE_KEY, dsd);
	}

	private void initFromSysProperties() {
		String url = System.getProperty(DB_URL);
		String type = System.getProperty(DB_TYPE);
		String user = System.getProperty(DB_USER);
		String pass = System.getProperty(DB_PASS);
		String maxConn = System.getProperty(DB_MAX_CONN);
		String appName = System.getProperty(DB_APP_NAME);

		DataSourceDetails dsd = createDataSource(url, type, user, pass, maxConn, appName);
		dataSourcesRegistry.put(DataSourceProvider.DEFAULT_DATASOURCE_KEY, dsd);
	}

	private void initFromProperties(String configLocation) {
		String[] configKeys = new String[]{DB_URL, DB_TYPE, DB_USER, DB_PASS, DB_MAX_CONN, DB_APP_NAME};
		Map<String, Map<String, String>> configSets = new HashMap<>();
		Properties dbProps = new Properties();
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(configLocation)) {
			dbProps.load(is);

			//  collect all properties into sets by keys
			for (Map.Entry<Object, Object> p : dbProps.entrySet()) {
				String key = (String) p.getKey();
				String val = (String) p.getValue();
				for (String configKey : configKeys) {
					if (key.endsWith(configKey)) {
						String configSetKey = key.replace(configKey, "");
						configSetKey = configSetKey.endsWith(".") ? configSetKey.substring(0, configSetKey.length() - 1) : configSetKey;
						configSets
								.computeIfAbsent(configSetKey, k -> new HashMap<>())
								.put(configKey, val);
						break;
					}
				}
			}

			//  process special default case (empty key); validate duplication with an explicit "default" config set
			if (configSets.containsKey("")) {
				Map<String, String> implicitlyDefaultSet = configSets.remove("");
				if (!configSets.containsKey(DataSourceProvider.DEFAULT_DATASOURCE_KEY)) {
					configSets.put(DataSourceProvider.DEFAULT_DATASOURCE_KEY, implicitlyDefaultSet);
				} else {
					logger.warn("configuration '" + configLocation +
							"' specified duplicate default configuration, once explicitly by key 'default' and once implicitly by empty key; ONLY explicit configuration will be taken");
				}
			}

			//  create data sources by config sets
			for (Map.Entry<String, Map<String, String>> configSet : configSets.entrySet()) {
				String configSetKey = configSet.getKey();
				Map<String, String> configProps = configSet.getValue();
				String url = configProps.get(DB_URL);
				String type = configProps.get(DB_TYPE);
				String user = configProps.get(DB_USER);
				String pass = configProps.get(DB_PASS);
				String maxConn = configProps.get(DB_MAX_CONN);
				String appName = configProps.get(DB_APP_NAME);
				if (url == null || url.isEmpty()) {
					logger.error((configSetKey.isEmpty() ? "default" : configSetKey) + " configuration missing 'db.url', won't be used");
					break;
				}
				if (type == null || type.isEmpty()) {
					logger.error((configSetKey.isEmpty() ? "default" : configSetKey) + " configuration missing 'db.type', won't be used");
					break;
				}
				if (user == null || user.isEmpty()) {
					logger.error((configSetKey.isEmpty() ? "default" : configSetKey) + " configuration missing 'db.user', won't be used");
					break;
				}

				DataSourceDetails tmp = createDataSource(url, type, user, pass, maxConn, appName);
				dataSourcesRegistry.put(configSetKey, tmp);
			}
		} catch (IOException ioe) {
			throw new IllegalStateException("failed to initialize data sources configuration from '" + configLocation + "'");
		}
	}

	private DataSourceDetails createDataSource(String url, String type, String user, String pass, String maxConn, String appName) {
		if (url == null || url.isEmpty()) {
			throw new IllegalArgumentException("db configuration MUST have 'db.url' parameter");
		}
		if (type == null || type.isEmpty()) {
			throw new IllegalArgumentException("db configuration MUST have 'db.type' parameter");
		}
		if (user == null || user.isEmpty()) {
			throw new IllegalArgumentException("db configuration MUST have 'db.user' parameter");
		}
		if (pass == null) {
			throw new IllegalArgumentException("db configuration MUST have 'db.pass' parameter");
		}

		HikariDataSource ds = new HikariDataSource();
		ds.setJdbcUrl(url);
		ds.setDriverClassName(getDriverClassByDBType(type));
		ds.setUsername(user);
		ds.setPassword(pass);
		if (maxConn != null) {
			try {
				int mc = Integer.parseInt(maxConn);
				ds.setMaximumPoolSize(mc);
			} catch (NumberFormatException nfe) {
				logger.warn("failed to parse max connection parameter '" + maxConn + "', will keep the default");
			}
		}
		if (appName != null && !appName.isEmpty()) {
			ds.addDataSourceProperty("ApplicationName", appName);
		}
		ds.validate();
		return new DataSourceDetailsImpl(type, url, user, pass, ds);
	}

	private String getDriverClassByDBType(String dbType) {
		String result = null;
		if ("postgres".equals(dbType)) {
			result = "org.postgresql.Driver";
		}
		if (result == null) {
			throw new IllegalStateException("DB type '" + dbType + "' is not supported");
		}
		return result;
	}
}
