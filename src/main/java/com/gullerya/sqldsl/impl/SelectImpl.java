package com.gullerya.sql.impl;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

class SelectImpl<T> implements
		Select<T>,
		Select.SelectDownstream<T>,
		Select.WhereDownstream<T>,
		Select.GroupByDownstream<T>,
		Select.OrderByDownstream<T> {
	private final EntityService.ESConfig<T> config;
	private Set<String> selectedFields;
	private Where.WhereClause where;
	private Set<String> groupBy;
	private Set<OrderBy.OrderByClause> orderBy;

	SelectImpl(EntityService.ESConfig<T> config) {
		this.config = config;
	}

	@Override
	public SelectDownstream<T> select(String... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("fields MUST NOT be NULL nor EMPTY");
		}

		Set<String> tmp = new TreeSet<>();
		for (String f : fields) {
			if (f == null || f.isEmpty()) {
				throw new IllegalArgumentException("field MUST NOT be NULL nor EMPTY");
			}
			if (!config.em.byColumn.containsKey(f)) {
				throw new IllegalArgumentException("field '" + f + "' not found in entity " + config.em.type + " definition");
			}
			tmp.add(f);
		}
		this.selectedFields = tmp;
		return this;
	}

	@Override
	public SelectDownstream<T> select(Set<String> fields) {
		if (fields == null) {
			throw new IllegalArgumentException("fields MUST NOT be NULL");
		}
		return select(fields.toArray(new String[0]));
	}

	@Override
	public WhereDownstream<T> where(WhereClause where) {
		Where.validate(config.em, where);
		this.where = where;
		return this;
	}

	@Override
	public GroupByDownstream<T> groupBy(String... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("group by fields MUST NOT be NULL nor EMPTY");
		}
		Set<String> tmp = new LinkedHashSet<>(Arrays.asList(fields));
		GroupBy.validate(config.em, selectedFields, tmp);
		this.groupBy = tmp;
		return this;
	}

	@Override
	public HavingDownstream<T> having(String... fields) {
		throw new IllegalStateException("not implemented");
	}

	@Override
	public OrderByDownstream<T> orderBy(OrderByClause orderBy, OrderByClause... orderByMore) {
		if (orderBy == null) {
			throw new IllegalArgumentException("order by clause/s MUST NOT be NULL");
		}
		Set<OrderByClause> tmp = new LinkedHashSet<>();
		tmp.add(orderBy);
		if (orderByMore != null && orderByMore.length > 0) {
			tmp.addAll(Arrays.asList(orderByMore));
		}
		OrderBy.validate(config.em, groupBy, tmp);
		this.orderBy = tmp;
		return this;
	}

	@Override
	public T readSingle() {
		List<T> asList = internalRead(null, null);
		if (asList.isEmpty()) {
			return null;
		} else if (asList.size() == 1) {
			return asList.get(0);
		} else {
			throw new IllegalStateException(asList.size() + " results received while expected for at most 1");
		}
	}

	@Override
	public List<T> read() {
		return internalRead(null, null);
	}

	@Override
	public List<T> read(int limit) {
		if (limit == 0) {
			throw new IllegalArgumentException("limit MUST be greater than 0");
		}
		return internalRead(null, limit);
	}

	@Override
	public List<T> read(int offset, int limit) {
		if (offset == 0) {
			throw new IllegalArgumentException("offset MUST be greater than 0 ('read' methods without offset exists)");
		}
		if (limit == 0) {
			throw new IllegalArgumentException("limit MUST be greater than 0");
		}
		return internalRead(offset, limit);
	}

	private List<T> internalRead(Integer offset, Integer limit) {
		String sql = "SELECT " + buildFieldsClause(config.em.fqSchemaTableName) + " FROM " + config.em.fqSchemaTableName;
		Collection<WhereFieldValuePair> parametersCollector = new ArrayList<>();
		if (where != null) {
			sql += " WHERE " + where.stringify(parametersCollector);
		}
		if (groupBy != null) {
			sql += " GROUP BY " + String.join(",", groupBy);
		}
		if (orderBy != null) {
			sql += buildOrderByClause();
		}
		if (offset != null) {
			sql += " OFFSET " + offset + " ROWS";
		}
		if (limit != null && limit != Integer.MAX_VALUE) {
			sql += " FETCH FIRST " + limit + " ROWS ONLY";
		}

		return config.preparedStatementAndDo(sql, s -> {
			int i = 0;
			for (WhereFieldValuePair parameter : parametersCollector) {
				i++;
				EntityService.FieldMetadata fm = config.em.byColumn.get(parameter.column);
				if (fm.jdbcConverter != null) {
					fm.jdbcConverter.toDB(s, i, parameter.value);
				} else {
					s.setObject(i, fm.typeConverter.toDB(parameter.value));
				}
			}
			try (ResultSet rs = s.executeQuery()) {
				return translateDBRow(rs);
			}
		});
	}

	private String buildFieldsClause(String table) {
		if (selectedFields != null && !selectedFields.isEmpty()) {
			return String.join(",", selectedFields.stream().map(f -> table + "." + f).collect(Collectors.toSet()));
		} else {
			return "";
		}
	}

	private String buildOrderByClause() {
		return " ORDER BY " + orderBy.stream().map(obs -> obs.field + " " + obs.direction).collect(Collectors.joining(","));
	}

	private List<T> translateDBRow(ResultSet rs) throws SQLException {
		List<T> result = new ArrayList<>();

		try {
			Constructor<T> ctor = config.em.type.getDeclaredConstructor();
			while (rs.next()) {
				T tmp = ctor.newInstance();
				for (String f : selectedFields) {
					EntityService.FieldMetadata fm = config.em.byColumn.get(f);
					String colName = fm.fieldMetadata.value();
					Object dbValue;

					if (fm.jdbcConverter != null) {
						dbValue = fm.jdbcConverter.fromDB(rs, rs.findColumn(f));
					} else if (!(fm.typeConverter instanceof EntityField.DefaultTypeConverter)) {
						dbValue = rs.getObject(colName, fm.typeConverter.getDbType());
						dbValue = fm.typeConverter.fromDB(dbValue);
					} else {
						if (fm.field.getType().isArray()) {
							dbValue = rs.getBytes(colName);
						} else if (fm.field.getType().isAssignableFrom(InputStream.class)) {
							dbValue = rs.getBinaryStream(colName);
						} else {
							dbValue = rs.getObject(colName, fm.field.getType());
						}
					}

					fm.field.set(tmp, dbValue);
				}
				result.add(tmp);
			}

			return result;
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to build result entities", roe);
		}
	}
}
