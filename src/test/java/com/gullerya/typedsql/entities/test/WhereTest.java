package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.entities.EntitiesService;
import com.gullerya.typedsql.entities.Entity;
import com.gullerya.typedsql.entities.EntityField;
import com.gullerya.typedsql.entities.Where;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;

public class WhereTest {
	private final DataSource dataSource = DataSourceProvider.getInstance().getDataSourceDetails().getDataSource();

	@Test(expected = IllegalArgumentException.class)
	public void negative1() {
		Where.eq(null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negative2() {
		Where.eq("", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negative3() {
		Where.isNull(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negative4() {
		Where.isNotNull("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void negative5() {
		Where.eq("some", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negative6() {
		Where.not(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA1() {
		Where.between("some", null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA2() {
		Where.between("some", 5, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB1() {
		Where.in("some", (Object[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB2() {
		Object[] vs = new Object[0];
		Where.in("some", vs);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB3() {
		Where.in("some", (Collection<Object>) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB4() {
		Where.in("some", new ArrayList<>());
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC4() {
		EntitiesService.of(TestEnt.class, dataSource).select("some")
				.where(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC5() {
		EntitiesService.of(TestEnt.class, dataSource).select("some")
				.where(Where.eq("none", 6));
	}

	@Entity("some")
	public static class TestEnt {
		@EntityField("some")
		public String some;
	}
}
