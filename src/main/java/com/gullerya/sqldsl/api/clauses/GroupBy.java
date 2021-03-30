package com.gullerya.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface GroupBy<DS> {

	DS groupBy(String... fields);

	/**
	 * clause validator
	 */
	static <T> void validate(EntityService.EntityMetadata<T> em, Set<String> selectedFields, Set<String> groupByFields) {
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
				throw new IllegalArgumentException("field/s [" + String.join(", ", ill) + "] is/are selected, but NOT found in the GROUP BY clause");
			}
		}
	}
}
