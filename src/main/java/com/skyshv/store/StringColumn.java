package com.skyshv.store;

import com.skyshv.Buffer.BlockBuffer;
import com.skyshv.utils.ITransfer;


public class StringColumn {
    BlockBuffer<StrEntry> buffer = new BlockBuffer<>(StrEntry.class, null );
    String columnName;

    public void set(long rowidx, String value, int zorder) {
        final StrEntry strEntry = new StrEntry();
        strEntry.value = value;
        strEntry.zorder = zorder;
        buffer.set(rowidx, strEntry);
    }

    public StrEntry get(long rowidx){
        return buffer.get(rowidx);
    }
    public static class StrEntry implements ITransfer<StrEntry> {
        String value;
        int zorder;

        @Override
        public boolean Overwritable(StrEntry oldv) {
            return oldv == null || oldv.zorder <= zorder;
        }
    }
}
