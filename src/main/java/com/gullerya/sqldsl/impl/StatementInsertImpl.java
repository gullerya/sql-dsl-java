package com.gullerya.sqldsl.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.gullerya.sqldsl.Literal;
import com.gullerya.sqldsl.api.statements.Insert;

public class StatementInsertImpl<ET> implements Insert<ET> {
	private final EntityDALImpl.ESConfig<ET> config;

	StatementInsertImpl(EntityDALImpl.ESConfig<ET> config) {
		this.config = config;
	}

	@Override
	public int insert(ET entity, Literal... literals) {
		if (entity == null) {
			throw new IllegalArgumentException("inserted entity MUST NOT be NULL");
		}
		Map<String, String> literalsMap = validateCollect(literals);
		List<Map.Entry<FieldMetaProc, Object>> params = new ArrayList<>();
		String sql = buildInsertSet(entity, literalsMap, params);
		return config.prepareStatementAndDo(sql, s -> {
			for (int i = 0; i < params.size(); i++) {
				Map.Entry<FieldMetaProc, Object> paramValue = params.get(i);
				FieldMetaProc fmp = paramValue.getKey();
				Object v = paramValue.getValue();
				fmp.setColumnValue(s, i + 1, v);
			}
			return s.executeUpdate();
		});
	}

	@Override
	public int[] insert(Collection<ET> entities, Literal... literals) {
		if (entities == null || entities.isEmpty()) {
			throw new IllegalArgumentException("entities list MUST NOT be NULL nor EMPTY");
		}
		Map<String, String> literalsMap = validateCollect(literals);
		List<Map.Entry<FieldMetaProc, Object[]>> paramsSets = new ArrayList<>();
		String sql = buildInsertSet(entities, literalsMap, paramsSets);
		return config.prepareStatementAndDo(sql, s -> {
			for (int ec = 0; ec < entities.size(); ec++) {
				for (int fc = 0; fc < paramsSets.size(); fc++) {
					Map.Entry<FieldMetaProc, Object[]> paramValue = paramsSets.get(fc);
					FieldMetaProc fmp = paramValue.getKey();
					Object v = paramValue.getValue()[ec];
					fmp.setColumnValue(s, fc + 1, v);
				}
				s.addBatch();
			}
			return s.executeBatch();
		});
	}

	private Map<String, String> validateCollect(Literal... literals) {
		Map<String, String> result = new TreeMap<>();
		if (literals != null && literals.length > 0) {
			for (Literal literal : literals) {
				if (!config.em.byColumn.containsKey(literal.field)) {
					throw new IllegalArgumentException("field '" + literal.field + "' is not found in entity " + config.em.type);
				}
				result.put(literal.field, literal.value);
			}
		}
		return result;
	}

	private String buildInsertSet(ET entity, Map<String, String> literals, List<Map.Entry<FieldMetaProc, Object>> params) {
		Set<String> nonNullSet = new LinkedHashSet<>();
		for (Entry<String, FieldMetaProc> e : config.em.byColumn.entrySet()) {
			String cName = e.getKey();
			FieldMetaProc fmp = e.getValue();
			if (literals.containsKey(cName)) {
				continue;
			}
			Object fv = fmp.getFieldValue(entity);
			if (fv != null) {
				nonNullSet.add(cName);
				params.add(new AbstractMap.SimpleEntry<>(fmp, fv));
			}
		}

		String fields = String.join(",", nonNullSet);
		String values = String.join(",", Collections.nCopies(nonNullSet.size(), "?"));
		if (!literals.isEmpty()) {
			fields += (nonNullSet.isEmpty() ? "" : ",") + String.join(",", literals.keySet());
			values += (nonNullSet.isEmpty() ? "" : ",") + String.join(",", literals.values());
		}
		return "INSERT INTO " + config.em.fqSchemaTableName + " (" + fields + ")" + " VALUES (" + values + ")";
	}

	private String buildInsertSet(Collection<ET> entities, Map<String, String> literals, List<Map.Entry<FieldMetaProc, Object[]>> params) {
		Map<String, Map.Entry<FieldMetaProc, Object[]>> nonNullSet = new LinkedHashMap<>();
		for (FieldMetaProc fm : config.em.byColumn.values()) {
			if (literals.containsKey(fm.columnName)) {
				continue;
			}
			int i = 0;
			for (ET entity : entities) {
				Object fv = fm.getFieldValue(entity);
				if (fv != null) {
					nonNullSet
							.computeIfAbsent(fm.columnName, c -> new AbstractMap.SimpleEntry<>(fm, new Object[entities.size()]))
							.getValue()[i] = fv;
				}
				i++;
			}
		}
		String fields = String.join(",", nonNullSet.keySet());
		String values = String.join(",", Collections.nCopies(nonNullSet.size(), "?"));
		params.addAll(nonNullSet.values());
		if (!literals.isEmpty()) {
			fields += "," + String.join(",", literals.keySet());
			values += "," + String.join(",", literals.values());
		}
		return "INSERT INTO " + config.em.fqSchemaTableName + " (" + fields + ")" + " VALUES (" + values + ")";
	}
}
