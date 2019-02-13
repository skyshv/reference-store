package com.skyshv.store;

import com.skyshv.utils.IColumnFormatter;
import com.skyshv.utils.RowIdGenerator;
import com.skyshv.utils.RowLocker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class StoreSchema {
    private StringColumn[] columns = new StringColumn[8];
    private String lastSourceSystem;
    private String lastSourceField;
    private StringColumn lastColumn;
    private int columnNums = 0;
    private RowIdGenerator idGen = new RowIdGenerator();
    private RowLocker locker = new RowLocker();

    private HashMap<String, HashMap<String, Integer>> sourceSystemFieldsColumnIdx = new HashMap<>();
    private HashMap<String, ArrayList<String>> sourceSystemKeyColumns = new HashMap<>();
    private HashMap<String, Integer> columnNameIdxMap = new HashMap<>();


    public void PrintResult(String... keyFields) {
        final Long rowId = idGen.getRowId(keyFields);
        StringBuilder sbHeader = new StringBuilder();
        StringBuilder record = new StringBuilder();

        for (int i = 0; i < columnNums; i++) {
            sbHeader.append(columns[i].getColumnName());
            record.append(columns[i].getFormatter().format(columns[i].get(rowId) ));
            if (i < columnNums - 1) {
                sbHeader.append("|");
                record.append("|");
            }
        }
        System.out.println(sbHeader.toString());
        System.out.println(record.toString());
    }

    public void consumeSourceFeed(String sourceSystem, String header, String record) {
        final String[] heads = header.split("\\|");
        String[][] bodys = new String[1][];
        bodys[0] = record.split("\\|");
        this.consumeSourceFeed(sourceSystem, heads, bodys);
    }

    public void consumeSourceFeed(String sourceSystem, String[] header, String[][] body) {
        HashMap<String, Integer> fieldNameFeedIdxMap = new HashMap<>();
        HashMap<String, Integer> fieldsColumnIndexMap = sourceSystemFieldsColumnIdx.get(sourceSystem);
        ArrayList<String> keyFields = sourceSystemKeyColumns.get(sourceSystem);

        for (int i = 0; i < header.length; i++) {
            fieldNameFeedIdxMap.put(header[i], i);
        }

        List<Integer> feedIndexColumnIdxMapping = Arrays.stream(header).map(e -> fieldsColumnIndexMap.get(e)).collect(Collectors.toList());
        List<Integer> keysFeedIdxes = keyFields.stream().map(e -> fieldNameFeedIdxMap.get(e)).collect(Collectors.toList());
        Arrays.stream(body).parallel()
                .forEach(e -> {
                    Long rowId = idGen.getRowId(e, keysFeedIdxes);
                    locker.aquire(rowId);
                    for (int i = 0; i < e.length; i++) {
                        Integer columnIdx = feedIndexColumnIdxMapping.get(i);
                        columns[columnIdx & 0xffff].set(rowId, e[i], columnIdx >> 16);
                    }
                    locker.release(rowId);

                });

    }

    public StoreBuilder builder() {
        return new StoreBuilder();
    }

    public class StoreBuilder {
        public SourceSystemBuilder withSourceSystem(String system) {
            sourceSystemFieldsColumnIdx.computeIfAbsent(system, e -> new HashMap<>());
            sourceSystemKeyColumns.computeIfAbsent(system, e -> new ArrayList<String>());
            lastSourceSystem = system;
            return new SourceSystemBuilder();
        }
    }

    public class SourceSystemBuilder {
        public SourceSystemBuilder withStringField(String fieldName, String columnName, int zorder) {
            if (!columnNameIdxMap.containsKey(columnName)) {
                columnNameIdxMap.put(columnName, columnNums);
                columnNums++;
                if (columnNums > columns.length) {
                    StringColumn[] newColumns = new StringColumn[columns.length << 1];
                    System.arraycopy(columns, 0, newColumns, 0, columns.length);
                    columns = newColumns;
                }
                final StringColumn stringColumn = new StringColumn(columnName);
                columns[columnNums - 1] = stringColumn;

            }
            lastColumn = columns[columnNameIdxMap.get(columnName)];
            HashMap<String, Integer> fieldMaps = sourceSystemFieldsColumnIdx.get(lastSourceSystem);
            if (fieldMaps.containsKey(fieldName)) {
                throw new RuntimeException("duplicate fieldname " + lastSourceSystem + "/" + fieldName);
            }
            fieldMaps.put(fieldName, (columnNameIdxMap.get(columnName) & 0xffff) | (zorder << 16));
            lastSourceField = fieldName;
            return this;
        }

        public SourceSystemBuilder asKeyField() {
            ArrayList<String> keyfieldLists = sourceSystemKeyColumns.get(lastSourceSystem);
            keyfieldLists.add(lastSourceField);
            return this;
        }

        public SourceSystemBuilder addFormatter(IColumnFormatter<StringColumn.StrEntry> iColumnFormatter) {
            lastColumn.setFormatter(iColumnFormatter);
            return this;
        }
        public SourceSystemBuilder withSourceSystem(String system) {
            return new StoreBuilder().withSourceSystem(system);
        }

        public StoreSchema build() {
            return StoreSchema.this;
        }

    }

}
