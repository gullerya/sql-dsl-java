package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.DBUtils;
import com.gullerya.typedsql.configuration.DataSourceDetails;
import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.configuration.DataSourceProviderSPI;
import com.gullerya.typedsql.entities.OrderBy;
import com.gullerya.typedsql.entities.EntityService;
import com.gullerya.typedsql.entities.EntityField;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.gullerya.typedsql.entities.Where.and;
import static com.gullerya.typedsql.entities.Where.eq;
import static com.gullerya.typedsql.entities.Where.in;
import static com.gullerya.typedsql.entities.Where.isNotNull;
import static com.gullerya.typedsql.entities.Where.isNull;
import static com.gullerya.typedsql.entities.Where.not;
import static com.gullerya.typedsql.entities.Where.notEq;
import static com.gullerya.typedsql.entities.Where.or;

public class SelectTest {
	private static final String TABLE_NAME = "ReadTestTable";
	private static final DataSourceProviderSPI config = new DataSourceProviderSPI() {
		@Override
		public String getDBConfigLocation() {
			return "test.db.properties";
		}
	};
	private static final DataSourceDetails dsd = DataSourceProvider.getInstance(config).getDataSourceDetails();
	private static final DataSource dataSource = dsd.getDataSource();

	@BeforeClass
	public static void prepare() throws Exception {
		DBUtils.ensureSchema(dsd);
		dataSource.getConnection()
				.prepareStatement(
						"DROP TABLE IF EXISTS \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\";" +
								"CREATE TABLE \"" + DBUtils.DAL_TESTS_SCHEMA + "\".\"" + TABLE_NAME + "\" (" +
								"id         BIGINT," +
								"state      INT," +
								"ready      BOOLEAN," +
								"first_name VARCHAR(128)," +
								"last_name  VARCHAR(128)," +
								"birthday   DATE," +
								"created    TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', CURRENT_TIMESTAMP)," +
								"updated    TIMESTAMP," +
								"locales    VARCHAR(128)" +
								");"
				)
				.execute();
	}

	@Test
	public void testReadAll() {
		EntityService.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(1000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(1001L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(1002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(1003L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(1004L, 1, true, "Sam", "5", bday));
		int[] result = EntityService.of(User.class, dataSource).insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityService.of(User.class, dataSource)
				.select("id", "state", "ready", "first_name", "birthday", "created")
				.read();

		Assert.assertNotNull(users);
		Assert.assertEquals(5, users.size());
		for (User user : users) {
			Assert.assertNotNull(user);
			Assert.assertNotNull(user.id);
			Assert.assertEquals(1, (int) user.status);
			Assert.assertTrue(user.ready);
			Assert.assertEquals(bday, user.birthday);
			Assert.assertNotNull(user.created);
		}
	}

	@Test
	public void testReadOne() {
		EntityService.of(User.class, dataSource).delete();

		User userToInsert = new User(2000L, 4, false, "Sam", "1", null);
		boolean ir = EntityService.of(User.class, dataSource).insert(userToInsert);
		Assert.assertTrue(ir);

		User user = EntityService.of(User.class, dataSource)
				.select("id", "state", "first_name", "birthday")
				.where(eq("id", 2000))
				.readSingle();

		Assert.assertNotNull(user);
		Assert.assertEquals(2000, (long) user.id);
		Assert.assertEquals(4, (int) user.status);
		Assert.assertEquals("Sam", user.firstName);
		Assert.assertNull(user.birthday);
	}

	@Test
	public void testReadManyNoLimit() {
		EntityService.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(3000L, 0, true, "Sam", "1", bday));
		usersToInsert.add(new User(3000L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(3000L, 0, true, "Sam", "3", bday));
		usersToInsert.add(new User(3001L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(3001L, 0, true, "Sam", "5", bday));
		int[] result = EntityService.of(User.class, dataSource).insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityService.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name")
				.where(and(eq("first_name", "Sam"), eq("state", 0)))
				.orderBy(OrderBy.desc("id"), OrderBy.asc("last_name"))
				.read();

		Assert.assertNotNull(users);
		Assert.assertEquals(3, users.size());
		for (User user : users) {
			Assert.assertNotNull(user);
			Assert.assertNotNull(user.id);
			Assert.assertEquals("Sam", user.firstName);
			Assert.assertEquals(0, (int) user.status);
			Assert.assertNotNull(user.lastName);
		}
		Assert.assertEquals(3001, (long) users.get(0).id);
		Assert.assertEquals("5", users.get(0).lastName);
		Assert.assertEquals(3000, (long) users.get(1).id);
		Assert.assertEquals("1", users.get(1).lastName);
		Assert.assertEquals(3000, (long) users.get(2).id);
		Assert.assertEquals("3", users.get(2).lastName);
	}

	@Test
	public void testReadManyWithLimitNoOffset() {
		EntityService.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(4001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(4002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(4003L, 3, true, "ESam", "3", bday));
		usersToInsert.add(new User(4004L, 1, true, "DSam", "4", bday));
		usersToInsert.add(new User(4005L, 3, true, "CSam", "5", bday));
		usersToInsert.add(new User(4006L, 3, true, "BSam", "6", bday));
		usersToInsert.add(new User(4007L, 0, true, "ASam", "7", bday));
		int[] result = EntityService.of(User.class, dataSource).insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityService.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name", "birthday")
				.where(not(eq("state", 3)))
				.orderBy(OrderBy.asc("first_name"))
				.read(2);

		Assert.assertNotNull(users);
		Assert.assertEquals(2, users.size());
		Assert.assertEquals(4007, (long) users.get(0).id);
		Assert.assertEquals(0, (long) users.get(0).status);
		Assert.assertEquals("ASam", users.get(0).firstName);
		Assert.assertEquals("7", users.get(0).lastName);
		Assert.assertEquals(bday, users.get(0).birthday);
		Assert.assertEquals(4004, (long) users.get(1).id);
		Assert.assertEquals(1, (long) users.get(1).status);
		Assert.assertEquals("DSam", users.get(1).firstName);
		Assert.assertEquals("4", users.get(1).lastName);
		Assert.assertEquals(bday, users.get(1).birthday);
	}

	@Test
	public void testReadManyWithOffsetNoLimit() {
		EntityService.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		String[] lns = new String[]{"en", "he", "ru"};
		usersToInsert.add(new User(5001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(5002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(5003L, 3, true, "ESam", "3", bday));
		usersToInsert.add(new User(5004L, 1, true, "Jim", "4", bday));
		usersToInsert.add(new User(5005L, 3, false, "Jim", "5", bday));
		usersToInsert.add(new User(5006L, 3, false, "BSam", "6", bday));
		usersToInsert.add(new User(5007L, 0, false, "ASam", "7", bday));
		usersToInsert.forEach(u -> u.locales = lns);
		int[] result = EntityService.of(User.class, dataSource).insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityService.of(User.class, dataSource)
				.select("id", "ready", "first_name", "last_name", "birthday", "locales")
				.where(or(eq("ready", true), notEq("first_name", "Jim")))
				.orderBy(OrderBy.desc("last_name"))
				.read(2, Integer.MAX_VALUE);

		Assert.assertNotNull(users);
		Assert.assertEquals(4, users.size());
		Assert.assertEquals(5004, (long) users.get(0).id);
		Assert.assertEquals(true, users.get(0).ready);
		Assert.assertEquals("Jim", users.get(0).firstName);
		Assert.assertEquals("4", users.get(0).lastName);
		Assert.assertEquals(bday, users.get(0).birthday);
		Assert.assertArrayEquals(lns, users.get(0).locales);

		Assert.assertEquals(5003, (long) users.get(1).id);
		Assert.assertEquals(true, users.get(1).ready);
		Assert.assertEquals("ESam", users.get(1).firstName);
		Assert.assertEquals("3", users.get(1).lastName);
		Assert.assertEquals(bday, users.get(1).birthday);
		Assert.assertArrayEquals(lns, users.get(1).locales);

		Assert.assertEquals(5002, (long) users.get(2).id);
		Assert.assertEquals(true, users.get(2).ready);
		Assert.assertEquals("FSam", users.get(2).firstName);
		Assert.assertEquals("2", users.get(2).lastName);
		Assert.assertEquals(bday, users.get(2).birthday);
		Assert.assertArrayEquals(lns, users.get(2).locales);

		Assert.assertEquals(5001, (long) users.get(3).id);
		Assert.assertEquals(true, users.get(3).ready);
		Assert.assertEquals("GSam", users.get(3).firstName);
		Assert.assertEquals("1", users.get(3).lastName);
		Assert.assertEquals(bday, users.get(3).birthday);
		Assert.assertArrayEquals(lns, users.get(3).locales);
	}

	@Test
	public void testReadManyWithLimitOffset() {
		EntityService.of(User.class, dataSource).delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		String[] lns = new String[]{"en", "he", "ru"};
		usersToInsert.add(new User(6001L, 0, true, "GSam", "1", bday));
		usersToInsert.add(new User(6002L, 3, true, "FSam", "2", bday));
		usersToInsert.add(new User(6003L, 1, true, "ESam", "3", bday));
		usersToInsert.add(new User(6004L, 3, true, "Jim", "4", bday));
		usersToInsert.add(new User(6005L, 3, false, "Jim", "5", bday));
		usersToInsert.add(new User(6006L, 3, false, "BSam", "6", bday));
		usersToInsert.add(new User(6007L, 0, false, "ASam", "7", bday));
		usersToInsert.forEach(u -> u.locales = lns);
		int[] result = EntityService.of(User.class, dataSource).insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		List<User> users = EntityService.of(User.class, dataSource)
				.select("id", "state", "first_name", "last_name", "birthday", "locales")
				.where(in("id", Arrays.asList(6001, 6003, 6004, 6007)))
				.orderBy(OrderBy.asc("state"), OrderBy.desc("last_name"))
				.read(1, 2);

		EntityService.of(User.class, dataSource).delete();

		Assert.assertNotNull(users);
		Assert.assertEquals(2, users.size());
		Assert.assertEquals(6001, (long) users.get(0).id);
		Assert.assertEquals(0, (long) users.get(0).status);
		Assert.assertEquals("GSam", users.get(0).firstName);
		Assert.assertEquals("1", users.get(0).lastName);
		Assert.assertEquals(bday, users.get(0).birthday);
		Assert.assertArrayEquals(lns, users.get(0).locales);

		Assert.assertEquals(6003, (long) users.get(1).id);
		Assert.assertEquals(1, (long) users.get(1).status);
		Assert.assertEquals("ESam", users.get(1).firstName);
		Assert.assertEquals("3", users.get(1).lastName);
		Assert.assertEquals(bday, users.get(1).birthday);
		Assert.assertArrayEquals(lns, users.get(1).locales);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA() {
		EntityService.of(User.class, dataSource).select();
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB() {
		EntityService.of(User.class, dataSource).select((String[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC() {
		String[] arg = new String[0];
		EntityService.of(User.class, dataSource).select(arg);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeD() {
		String[] arg = new String[]{"some", null};
		EntityService.of(User.class, dataSource).select(arg);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeE() {
		String[] arg = new String[]{"", "some"};
		EntityService.of(User.class, dataSource).select(arg);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeF() {
		String[] arg = new String[]{null, "some"};
		EntityService.of(User.class, dataSource).select(arg);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeG() {
		Set<String> fields = new HashSet<>();
		EntityService.of(User.class, dataSource).select(fields);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeH() {
		Set<String> fields = Collections.singleton(null);
		EntityService.of(User.class, dataSource).select(fields);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeI() {
		Set<String> fields = Collections.singleton("");
		EntityService.of(User.class, dataSource).select(fields);
	}

	@Test
	public void negativeJ1() {
		User r = EntityService.of(User.class, dataSource).select("id").where(eq("id", 9999)).readSingle();
		Assert.assertNull(r);
	}

	@Test(expected = IllegalStateException.class)
	public void negativeJ2() {
		List<User> ul = new ArrayList<>();
		ul.add(new User(7000L, 4, false, "Sam", "1", null));
		ul.add(new User(7000L, 4, false, "Sam", "1", null));
		int[] ir = EntityService.of(User.class, dataSource).insert(ul);
		Assert.assertTrue(IntStream.of(ir).allMatch(i -> i == 1));

		EntityService.of(User.class, dataSource)
				.select("id")
				.where(or(eq("ready", false), eq("ready", true), isNull("ready"), isNotNull("ready")))
				.readSingle();
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeK1() {
		EntityService.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeK2() {
		EntityService.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeK3() {
		EntityService.of(User.class, dataSource).select("id").where(eq("id", 9999)).read(1, 0);
	}

	@Entity
	@Table(name = TABLE_NAME, schema = DBUtils.DAL_TESTS_SCHEMA)
	public static final class User {
		@EntityField(value = "id", readonly = true, nullable = false)
		public Long id;
		@EntityField("state")
		public Integer status;
		@EntityField("ready")
		public Boolean ready;
		@EntityField("first_name")
		public String firstName;
		@EntityField("last_name")
		public String lastName;
		@EntityField("birthday")
		public LocalDate birthday;
		@EntityField(value = "created", readonly = true)
		public LocalDateTime created;
		@EntityField("updated")
		public LocalDateTime updated;
		@EntityField(value = "locales", typeConverter = LocalesConverter.class)
		public String[] locales;

		public User() {
		}

		User(Long id, Integer status, Boolean ready, String firstName, String lastName, LocalDate birthday) {
			this.id = id;
			this.status = status;
			this.ready = ready;
			this.firstName = firstName;
			this.lastName = lastName;
			this.birthday = birthday;
		}

		public static final class LocalesConverter implements EntityField.TypeConverter<String, String[]> {
			@Override
			public String toDB(String[] input) {
				return input == null ? null : String.join(",", input);
			}

			@Override
			public String[] fromDB(String object) {
				return object == null ? null : object.split(",");
			}

			@Override
			public Class<String> getDbType() {
				return String.class;
			}
		}
	}
}
