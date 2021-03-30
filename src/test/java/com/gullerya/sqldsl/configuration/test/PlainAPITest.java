package com.gullerya.sqldsl.configuration.test;

import org.junit.Assert;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import com.gullerya.sql._configuration.DataSourceProvider;
import com.gullerya.sql._configuration.DataSourceProviderSPI;

public class PlainAPITest {

	@Test
	public void testA() {
		DataSourceProvider dsp1 = DataSourceProvider.getInstance();

		Assert.assertNotNull(dsp1);
		Assert.assertNotNull(dsp1.getDataSourceDetails());
		Assert.assertEquals("postgres", dsp1.getDataSourceDetails().getDBType());

		DataSourceProvider dsp2 = DataSourceProvider.getInstance();
		Assert.assertEquals(dsp1, dsp2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testANegative() {
		DataSourceProvider dsp1 = DataSourceProvider.getInstance(null);
	}

	@Test
	public void testB() {
		DataSourceProviderSPI configurer = new DataSourceProviderSPI() {
			@Override
			public String getDBConfigLocation() {
				return "test1.db.properties";
			}
		};
		DataSourceProvider dsp1 = DataSourceProvider.getInstance(configurer);

		Assert.assertNotNull(dsp1);
		Assert.assertNotNull(dsp1.getDataSourceDetails());
		Assert.assertEquals("postgres", dsp1.getDataSourceDetails().getDBType());

		DataSourceProvider dsp2 = DataSourceProvider.getInstance(configurer);
		Assert.assertEquals(dsp1, dsp2);
	}

	@Test
	public void testC() {
		DataSourceProvider dsp = DataSourceProvider.getInstance(new DataSourceProviderSPI() {
			@Override
			public String getDBConfigLocation() {
				return "test2.db.properties";
			}
		});

		Assert.assertNotNull(dsp);
		Assert.assertNull(dsp.getDataSourceDetails());

		Assert.assertNotNull(dsp.getDataSourceDetails("key1"));
		Assert.assertEquals("postgres", dsp.getDataSourceDetails("key1").getDBType());

		Assert.assertNotNull(dsp.getDataSourceDetails("key2"));
		Assert.assertEquals("postgres", dsp.getDataSourceDetails("key2").getDBType());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCNegativeA() {
		DataSourceProvider dsp = DataSourceProvider.getInstance(new DataSourceProviderSPI() {
			@Override
			public String getDBConfigLocation() {
				return "test2.db.properties";
			}
		});

		Assert.assertNotNull(dsp);
		dsp.getDataSourceDetails(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCNegativeB() {
		DataSourceProvider dsp = DataSourceProvider.getInstance(new DataSourceProviderSPI() {
			@Override
			public String getDBConfigLocation() {
				return "test2.db.properties";
			}
		});

		Assert.assertNotNull(dsp);
		dsp.getDataSourceDetails("");
	}
}
