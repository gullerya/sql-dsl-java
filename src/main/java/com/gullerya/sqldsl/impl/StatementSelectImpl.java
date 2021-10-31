package com.gullerya.sqldsl.impl;

import com.gullerya.sqldsl.api.clauses.OrderBy;
import com.gullerya.sqldsl.api.clauses.Where;
import com.gullerya.sqldsl.api.statements.Select;

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

public class StatementSelectImpl<T> implements Select<T>, Select.SelectDownstream<T>, Select.WhereDownstream<T>,
		Select.GroupByDownstream<T>, Select.OrderByDownstream<T> {
	private final EntityDALImpl.ESConfig<T> config;
	private Set<String> selectedFields;
	private Where.WhereClause where;
	private Set<String> groupBy;
	private Set<OrderBy.OrderByClause> orderBy;

	StatementSelectImpl(EntityDALImpl.ESConfig<T> config) {
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
			if (!config.em().byColumn.containsKey(f)) {
				throw new IllegalArgumentException(
						"field '" + f + "' not found in entity " + config.em().type + " definition");
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
		validateWhereClause(config.em(), where);
		this.where = where;
		return this;
	}

	@Override
	public GroupByDownstream<T> groupBy(String... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("group by fields MUST NOT be NULL nor EMPTY");
		}
		Set<String> tmp = new LinkedHashSet<>(Arrays.asList(fields));
		validateGroupByClause(config.em(), selectedFields, tmp);
		this.groupBy = tmp;
		return this;
	}

	@Override
	public HavingDownstream<T> having(String... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("having fields MUST NOT be NULL nor EMPTY");
		}
		Set<String> tmp = new LinkedHashSet<>(Arrays.asList(fields));
		validateHavingClause(config.em(), tmp);
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
		validateOrderByClause(config.em(), groupBy, tmp);
		this.orderBy = tmp;
		return this;
	}

	@Override
	public T readOne() {
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
	public List<T> readAll() {
		return internalRead(null, null);
	}

	@Override
	public List<T> readAll(int limit) {
		if (limit == 0) {
			throw new IllegalArgumentException("limit MUST be greater than 0");
		}
		return internalRead(null, limit);
	}

	@Override
	public List<T> readAll(int offset, int limit) {
		if (offset == 0) {
			throw new IllegalArgumentException("offset MUST be greater than 0 ('read' methods without offset exists)");
		}
		if (limit == 0) {
			throw new IllegalArgumentException("limit MUST be greater than 0");
		}
		return internalRead(offset, limit);
	}

	private List<T> internalRead(Integer offset, Integer limit) {
		String sql = "SELECT " + buildFieldsClause(config.em().fqSchemaTableName) + " FROM "
				+ config.em().fqSchemaTableName;
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

		return config.prepareStatementAndDo(sql, s -> {
			int i = 0;
			for (WhereFieldValuePair parameter : parametersCollector) {
				i++;
				FieldMetaProc fm = config.em().byColumn.get(parameter.column());
				Object pv = fm.translateFieldToColumn(parameter.value());
				s.setObject(i, pv);
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

	private void validateWhereClause(EntityMetaProc<T> em, WhereClause where) {
		if (where == null) {
			throw new IllegalArgumentException("where clause MUST NOT be NULL");
		}
		Collection<String> fields = where.collectFields();
		for (String f : fields) {
			if (!em.byColumn.containsKey(f)) {
				throw new IllegalArgumentException("field '" + f + "' not found in entity " +
						em.type + " definition");
			}
		}
	}

	private void validateGroupByClause(EntityMetaProc<T> em, Set<String> selectedFields, Set<String> groupByFields) {
		for (String f : groupByFields) {
			if (!em.byColumn.containsKey(f)) {
				throw new IllegalArgumentException("field '" + f + "' not found in entity " + em.type + " definition");
			}
		}
		if (selectedFields != null && !selectedFields.isEmpty()) {
			List<String> ill = new ArrayList<>();
			for (String sf : selectedFields) {
				if (!groupByFields.contains(sf)) {
					ill.add(sf);
				}
			}
			if (!ill.isEmpty()) {
				throw new IllegalArgumentException("field/s [" + String.join(", ", ill) + "] is / are selected, but NOT found in the GROUP BY clause");
			}
		}
	}

	private void validateHavingClause(EntityMetaProc<T> em, Set<String> fields) {
		for (String f : fields) {
			if (!em.byColumn.containsKey(f)) {
				throw new IllegalArgumentException("field '" + f + "' not found in entity " + em.type + " definition");
			}
		}
	}

	private void validateOrderByClause(EntityMetaProc<T> em, Set<String> groupByFields, Set<OrderByClause> orderByFields) {
		for (OrderByClause obc : orderByFields) {
			if (!em.byColumn.containsKey(obc.field)) {
				throw new IllegalArgumentException("field '" + obc.field + "' not found in entity " + em.type + " definition");
			}
		}
		if (groupByFields != null && !groupByFields.isEmpty()) {
			List<String> ill = new ArrayList<>();
			for (OrderByClause obc : orderByFields) {
				if (!groupByFields.contains(obc.field)) {
					ill.add(obc.field);
				}
			}
			if (!ill.isEmpty()) {
				throw new IllegalArgumentException("field/s [" + String.join(", ", ill) + "] is/are found in the ORDER BY clause, but NOT in the GROUP BY clause");
			}
		}
	}

	private String buildOrderByClause() {
		return " ORDER BY " + orderBy.stream().map(OrderByClause::toString).collect(Collectors.joining(","));
	}

	private List<T> translateDBRow(ResultSet rs) throws SQLException {
		List<T> result = new ArrayList<>();

		try {
			Constructor<T> ctor = config.em().type.getDeclaredConstructor();
			while (rs.next()) {
				T tmp = ctor.newInstance();
				for (String f : selectedFields) {
					FieldMetaProc fm = config.em().byColumn.get(f);
					String colName = fm.columnName;
					Object dbValue = fm.getColumnValue(rs, colName);
					fm.setFieldValue(tmp, dbValue);
				}
				result.add(tmp);
			}

			return result;
		} catch (ReflectiveOperationException roe) {
			throw new IllegalStateException("failed to build result entities", roe);
		}
	}
}
