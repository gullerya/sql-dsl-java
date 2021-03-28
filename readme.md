# DB Configuration

This unit is called to provide DB configuration and `DataSource` object for any service, that needs a DB access.

The main entry point is the following static factory method:
```
DataSourceProvider dsp = DataSourceProvider.getInstance(<DataSourceProviderSPI implementation>);
```

One may obtain `DataSourceProvider` with some configuration or without, thus falling back to the default one.
The default one will always attempt to read config from the `db.properties` file found in the classpath.

Usually though, one would like to implement `DataSourceProviderSPI` and there specify where the config location should be taken from, overriding the following method:
```
String configLocation = dspSPI.getDBConfigLocation();
```

The `configLocation` string above will be taken as a path to the properties file resource on the classpath.

Yet, there are few special options that will be treated in a special way:
* NULL - the `configLocation` self will be resolved in the following search path:
    * take it from __environment variable__ named `db.config.location`, if present; otherwise
    * take if from __system property__ named `db.config.location`, if present
* `environment` - DB configuration will be looked up from environment
* `sysprops` - DB configuration will be looked up from system properties (aka VM options, `-D` params)

### DB config parameters

Any DB configuration consists of the following set of parameters, some of them are required, others - optional.

##### Required
* `db.url` - valid JDBC URL including the DB name (for example `jdbc:postgresql://localhost:5432/lessonomy`)
* `db.type` - as of now, ONLY `postgres` value issupported
* `db.user` - username
* `db.pass` - password


##### Optional
* `db.maxConn` - max connection to allow (defaults to 10 as of now, but may change due to connection pool provider defaults in the following versions)
* `db.appName` - application name, that will appear as part of the connections metadata