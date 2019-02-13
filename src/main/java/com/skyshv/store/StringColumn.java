package com.skyshv.store;

import com.skyshv.Buffer.BlockBuffer;
import com.skyshv.utils.IColumnFormatter;
import com.skyshv.utils.ITransfer;


public class StringColumn {
    BlockBuffer<StrEntry> buffer = new BlockBuffer<>(StrEntry.class, null );
    private String columnName;
    private IColumnFormatter<StrEntry> formatter;

    StringColumn(String columnName){
        this.columnName = columnName;
        formatter = new IColumnFormatter<StrEntry>() {
            @Override
            public String format(StrEntry value) {
                if( value == null ) return "null";
                return  value.value;
            }
        };
    }

    public IColumnFormatter<StrEntry> getFormatter() {
        return formatter;
    }

    public void setFormatter(IColumnFormatter<StrEntry> formatter) {
        this.formatter = formatter;
    }


    public void set(long rowidx, String value, int zorder) {
        final StrEntry strEntry = new StrEntry();
        strEntry.value = value;
        strEntry.zorder = zorder;
        buffer.set(rowidx, strEntry);
    }

    public StrEntry get(long rowidx){
        return buffer.get(rowidx);
    }

    public String getColumnName() {
        return columnName;
    }

    public static class StrEntry implements ITransfer<StrEntry> {
        private String value;
        private int zorder;

        public String getValue() {
            return value;
        }

        @Override
        public boolean Overwritable(StrEntry oldv) {
            return oldv == null || oldv.zorder <= zorder;
        }
    }
}
