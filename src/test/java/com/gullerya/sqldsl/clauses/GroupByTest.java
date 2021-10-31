package com.gullerya.sqldsl.clauses;

import com.gullerya.sqldsl.DBUtils;
import com.gullerya.sqldsl.EntityDAL;
import com.gullerya.sqldsl.api.clauses.OrderBy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class GroupByTest {
	private static final String TABLE_NAME = "GroupByTestTable";
	private static final DataSource dataSource = DBUtils.getDataSource();
	private final EntityDAL<Book> es = EntityDAL.of(Book.class, dataSource);

	@BeforeAll
	public static void prepare() throws Exception {
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + TABLE_NAME + "\" (" +
								"id         BIGINT," +
								"author     VARCHAR(32)," +
								"price      DECIMAL(15,4)" +
								");"
				)
				.execute();
	}

	@Test
	public void testGroupByOne() {
		es.deleteAll();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(1000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(1001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(1002L, "SamA", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(1003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(1004L, "SamC", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("author")
				.groupBy("author")
				.orderBy(OrderBy.asc("author"))
				.readAll();

		assertNotNull(users);
		assertEquals(3, users.size());
		assertEquals("SamA", users.get(0).author);
		assertEquals("SamB", users.get(1).author);
		assertEquals("SamC", users.get(2).author);
	}

	@Test
	public void testGroupByManyA() {
		es.deleteAll();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(2000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2002L, "SamA", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(2003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(2004L, "SamC", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("author", "price")
				.groupBy("author", "price")
				.orderBy(OrderBy.asc("author"), OrderBy.desc("price"))
				.readAll();

		assertNotNull(users);
		assertEquals(4, users.size());
		assertEquals("SamA", users.get(0).author);
		assertEquals(5, users.get(0).price.doubleValue(), 0);
		assertEquals("SamA", users.get(1).author);
		assertEquals(2.4, users.get(1).price.doubleValue(), 0);
		assertEquals("SamB", users.get(2).author);
		assertEquals(7, users.get(2).price.doubleValue(), 0);
		assertEquals("SamC", users.get(3).author);
		assertEquals(7, users.get(3).price.doubleValue(), 0);
	}

	@Test
	public void testGroupByManyB() {
		es.deleteAll();

		List<Book> usersToInsert = new ArrayList<>();
		usersToInsert.add(new Book(2000L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2001L, "SamA", BigDecimal.valueOf(2.4)));
		usersToInsert.add(new Book(2002L, "SamB", BigDecimal.valueOf(5)));
		usersToInsert.add(new Book(2003L, "SamB", BigDecimal.valueOf(7)));
		usersToInsert.add(new Book(2004L, "SamB", BigDecimal.valueOf(7)));
		int[] result = es.insert(usersToInsert);
		assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<Book> users = es.select("price", "author")
				.groupBy("price", "author")
				.orderBy(OrderBy.desc("author"), OrderBy.asc("price"))
				.readAll();

		assertNotNull(users);
		assertEquals(3, users.size());
		assertEquals("SamB", users.get(0).author);
		assertEquals(5, users.get(0).price.doubleValue(), 0);
		assertEquals("SamB", users.get(1).author);
		assertEquals(7, users.get(1).price.doubleValue(), 0);
		assertEquals("SamA", users.get(2).author);
		assertEquals(2.4, users.get(2).price.doubleValue(), 0);
	}

	@Test
	public void negativeA1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.select("id").groupBy((String[]) null)
		);
	}

	@Test
	public void negativeA2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> {
					String[] gb = new String[0];
					es.select("id").groupBy(gb);
				}
		);
	}

	@Test
	public void negativeB1() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.select("id").groupBy("none")
		);
	}

	@Test
	public void negativeB2() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.select("id").groupBy("author")
		);
	}

	@Test
	public void negativeB3() {
		assertThrows(
				IllegalArgumentException.class,
				() -> es.select("id").groupBy("id").orderBy(OrderBy.asc("author"))
		);
	}

	@Entity
	@Table(name = TABLE_NAME)
	public static final class Book {
		@Column(updatable = false)
		public Long id;
		@Column
		public String author;
		@Column
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
