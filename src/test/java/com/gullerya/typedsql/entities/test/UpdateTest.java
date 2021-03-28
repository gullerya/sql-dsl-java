package com.gullerya.typedsql.entities.test;

import com.gullerya.typedsql.DBUtils;
import com.gullerya.typedsql.configuration.DataSourceDetails;
import com.gullerya.typedsql.configuration.DataSourceProvider;
import com.gullerya.typedsql.configuration.DataSourceProviderSPI;
import com.gullerya.typedsql.entities.EntitiesService;
import com.gullerya.typedsql.entities.EntityField;
import com.gullerya.typedsql.entities.Literal;
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
import java.util.List;
import java.util.stream.IntStream;

import static com.gullerya.typedsql.entities.OrderBy.asc;
import static com.gullerya.typedsql.entities.Where.and;
import static com.gullerya.typedsql.entities.Where.eq;
import static com.gullerya.typedsql.entities.Where.in;
import static com.gullerya.typedsql.entities.Where.or;

public class UpdateTest {
	private static final String TABLE_NAME = "UpdateTestTable";
	private static final DataSourceProviderSPI config = new DataSourceProviderSPI() {
		@Override
		public String getDBConfigLocation() {
			return "test.db.properties";
		}
	};
	private static final DataSourceDetails dsd = DataSourceProvider.getInstance(config).getDataSourceDetails();
	private static final DataSource dataSource = dsd.getDataSource();
	private static final EntitiesService<User> usersService = EntitiesService.of(User.class, dataSource);

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
	public void testUpdateAll() {
		usersService.delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(1000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(1001L, 1, true, "Sam", "2", bday));
		usersToInsert.add(new User(1002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(1003L, 1, true, "Sam", "4", bday));
		usersToInsert.add(new User(1004L, 1, true, "Sam", "5", bday));
		int[] result = usersService.insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		int updated = usersService
				.update(new User(null, 2, null, null, null, null))
				.all();
		Assert.assertEquals(5, updated);

		List<User> users = usersService.select(Collections.singleton("state")).read();

		Assert.assertNotNull(users);
		Assert.assertEquals(5, users.size());
		for (User user : users) {
			Assert.assertNotNull(user);
			Assert.assertEquals(2, (int) user.status);
		}
	}

	@Test
	public void testUpdateSome() {
		usersService.delete();

		List<User> usersToInsert = new ArrayList<>();
		LocalDate bday = LocalDate.now();
		usersToInsert.add(new User(2000L, 1, true, "Sam", "1", bday));
		usersToInsert.add(new User(2001L, 1, false, "Sam", "2", bday));
		usersToInsert.add(new User(2002L, 1, true, "Sam", "3", bday));
		usersToInsert.add(new User(2003L, 1, false, "Sam", "4", bday));
		usersToInsert.add(new User(2004L, 1, true, "Sam", "5", bday));
		int[] result = usersService.insert(usersToInsert);
		Assert.assertTrue(IntStream.of(result).allMatch(i -> i == 1));

		int updated = usersService
				.update(new User(null, 2, null, null, null, null))
				.where(or(Arrays.asList(eq("ready", false), and(eq("first_name", "Sam"), in("last_name", "1", "5")))));
		Assert.assertEquals(4, updated);

		List<User> users = usersService.select("id", "state").where(eq("state", 2)).orderBy(asc("id")).read();

		Assert.assertNotNull(users);
		Assert.assertEquals(4, users.size());
		for (User user : users) {
			Assert.assertNotNull(user);
			Assert.assertEquals(2, (int) user.status);
		}
		Assert.assertEquals(2000, (long) users.get(0).id);
		Assert.assertEquals(2001, (long) users.get(1).id);
		Assert.assertEquals(2003, (long) users.get(2).id);
		Assert.assertEquals(2004, (long) users.get(3).id);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeA() {
		usersService.update(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeB() {
		usersService.update(new User());
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC1() {
		Literal[] ls = new Literal[0];
		usersService.update(new User(), ls);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC2() {
		usersService.update(new User(), (Literal[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void negativeC3() {
		usersService.update(new User(), Literal.exp("none", "DEFAULT"));
	}

	@Entity
	@Table(name = TABLE_NAME, schema = DBUtils.DAL_TESTS_SCHEMA)
	public static final class User {
		@EntityField(value = "id", readonly = true)
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
