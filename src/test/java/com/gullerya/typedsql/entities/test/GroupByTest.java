package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.DBUtils;
import com.gullerya.typedsql.configuration.DataSourceDetails;
import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.configuration.DataSourceProviderSPI;
import com.gullerya.typedsql.entities.EntitiesService;
import com.gullerya.typedsql.entities.EntityField;
import com.gullerya.typedsql.entities.OrderBy;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class GroupByTest {
	private static final String TABLE_NAME = "GroupByTestTable";
	private static final DataSourceProviderSPI config = new DataSourceProviderSPI() {
		@Override
		public String getDBConfigLocation() {
			return "test.db.properties";
		}
	};
	private static final DataSourceDetails dsd = DataSourceProvider.getInstance(config).getDataSourceDetails();
	private static final DataSource dataSource = dsd.getDataSource();
	private final EntitiesService<Book> es = EntitiesService.of(Book.class, dataSource);

	@BeforeClass
	public static void prepare() throws Exception {
		DBUtils.ensureSchema(dsd);
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\" (" +
								"id         BIGINT," +
								"author     VARCHAR(32)," +
								"price      DECIMAL(15,4)" +
								");"
				)
				.execute();
	}

	@Test
	public void testGroupByOne() {
		es.delete();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(1000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(1001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(1002L, "SamA", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(1003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(1004L, "SamC", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("author")
				.groupBy("author")
				.orderBy(OrderBy.asc("author"))
				.read();

		Assert.assertNotNull(users);
		Assert.assertEquals(3, users.size());
		Assert.assertEquals("SamA", users.get(0).author);
		Assert.assertEquals("SamB", users.get(1).author);
		Assert.assertEquals("SamC", users.get(2).author);
	}

	@Test
	public void testGroupByManyA() {
		es.delete();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(2000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2002L, "SamA", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(2003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(2004L, "SamC", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("author", "price")
				.groupBy("author", "price")
				.orderBy(OrderBy.asc("author"), OrderBy.desc("price"))
				.read();

		Assert.assertNotNull(users);
		Assert.assertEquals(4, users.size());
		Assert.assertEquals("SamA", users.get(0).author);
		Assert.assertEquals(5, users.get(0).price.doubleValue(), 0);
		Assert.assertEquals("SamA", users.get(1).author);
		Assert.assertEquals(2.4, users.get(1).price.doubleValue(), 0);
		Assert.assertEquals("SamB", users.get(2).author);
		Assert.assertEquals(7, users.get(2).price.doubleValue(), 0);
		Assert.assertEquals("SamC", users.get(3).author);
		Assert.assertEquals(7, users.get(3).price.doubleValue(), 0);
	}

	@Test
	public void testGroupByManyB() {
		es.delete();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(2000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2002L, "SamB", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(2003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(2004L, "SamB", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("price", "author")
				.groupBy("price", "author")
				.orderBy(OrderBy.desc("author"), OrderBy.asc("price"))
				.read();

		Assert.assertNotNull(users);
		Assert.assertEquals(3, users.size());
		Assert.assertEquals("SamB", users.get(0).author);
		Assert.assertEquals(5, users.get(0).price.doubleValue(), 0);
		Assert.assertEquals("SamB", users.get(1).author);
		Assert.assertEquals(7, users.get(1).price.doubleValue(), 0);
		Assert.assertEquals("SamA", users.get(2).author);
		Assert.assertEquals(2.4, users.get(2).price.doubleValue(), 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA1() {
		es.select("id").groupBy((String[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA2() {
		String[] gb = new String[0];
		es.select("id").groupBy(gb);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB1() {
		es.select("id").groupBy("none");
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB2() {
		es.select("id").groupBy("author");
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB3() {
		es.select("id").groupBy("id").orderBy(OrderBy.asc("author"));
	}

	@Entity
	@Table(name = TABLE_NAME, schema = DBUtils.DAL_TESTS_SCHEMA)
	public static final class Book {
		@EntityField(value = "id", readonly = true)
		public Long id;
		@EntityField("author")
		public String author;
		@EntityField("price")
		public BigDecimal price;

		public Book() {
		}

		public Book(Long id, String author, BigDecimal price) {
			this.id = id;
			this.author = author;
			this.price = price;
		}
	}
}
