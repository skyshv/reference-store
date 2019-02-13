package com.skyshv.utils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RowIdGenerator {
    private ConcurrentHashMap<String, Long> idmaps = new ConcurrentHashMap<>();
    private AtomicLong nextIds = new AtomicLong(-1);

    public Long getRowId(String ...keyFields){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyFields.length; i++) {
            sb.append( keyFields[i].replaceAll("/","//"));
            sb.append("/");
        }
        return idmaps.computeIfAbsent( sb.toString(), e -> nextIds.incrementAndGet());

    }
    public Long getRowId(String []record, List<Integer> keyFeedIndex){
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < keyFeedIndex.size(); i++) {
            sb.append( record[keyFeedIndex.get(i)].replaceAll("/", "//"));
            sb.append("/");
        }
        return idmaps.computeIfAbsent( sb.toString(), e -> nextIds.incrementAndGet());
    }

}
