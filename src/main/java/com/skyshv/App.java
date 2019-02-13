package com.skyshv;

import com.skyshv.store.StoreSchema;
import com.skyshv.store.StringColumn;
import com.skyshv.utils.IColumnFormatter;

import java.util.Date;
import java.util.stream.IntStream;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        StoreSchema schema = new StoreSchema().builder().withSourceSystem("LME")
                .withStringField("LAST_TRADING_DATE", "LAST_TRADING_DATE", 1)
                .withStringField("DELIVERY_DATE", "DELIVERY_DATE", 1)
                .withStringField("MARKET", "MARKET", 0)
                .addFormatter(new IColumnFormatter<StringColumn.StrEntry>() {
                    @Override
                    public String format(StringColumn.StrEntry value) {
                        if( value == null || value.getValue() == null ) return "null";
                        if( value.getValue().startsWith("LME_")) return value.getValue().substring(4);
                        return value.getValue();

                    }
                })
                .withStringField("LABEL", "LABEL", 0)
                .withStringField("LME_CODE", "CODE", 0)
                .asKeyField()
                .withStringField("TRADABLE", "TRADABLE", 0)

                .withSourceSystem("PRIME")
                .withStringField("LAST_TRADING_DATE", "LAST_TRADING_DATE", 0)
                .withStringField("DELIVERY_DATE", "DELIVERY_DATE", 0)
                .withStringField("TRADABLE", "TRADABLE", 1)
                .withStringField("EXCHANGE_CODE", "CODE", 0)
                .asKeyField()
                .build();


        schema.consumeSourceFeed("PRIME", "LAST_TRADING_DATE|DELIVERY_DATE|TRADABLE|EXCHANGE_CODE", "15-02-2018| 15-03-2018|FALSE|PB_03_2018");
        schema.consumeSourceFeed("LME", "LME_CODE|LAST_TRADING_DATE|DELIVERY_DATE|MARKET|LABEL|TRADABLE", "PB_04_2018|15-03-2018| 17-03-2018|LME_PB|Lead 13 March 2018|TRUE");
        schema.consumeSourceFeed("LME", "LME_CODE|DELIVERY_DATE|MARKET|LABEL|TRADABLE", "PB_04_2018|17-03-2018|LME_PB|Lead 13 March 2018|TRUE");
        schema.PrintResult("PB_03_2018");
        schema.PrintResult("PB_04_2018");

        String header = "LME_CODE|LAST_TRADING_DATE|DELIVERY_DATE|MARKET|LABEL|TRADABLE";
        String value = "15-03-2018| 17-03-2018|LME_PB|Lead 13 March 2018|TRUE";
        final String[] vs = value.split("\\|");

        for( int i =0; i < 10; i++ ) {
            String[][] bodys = new String[1000000][];
            final int j = i;
            IntStream.range(0, bodys.length).parallel().forEach(e -> {

                final String[] strings = new String[6];
                bodys[e] = strings;
                strings[0] = String.valueOf((e + j * 1000000) % 777777);
                for(int k = 0; k < 5; k++){
                    strings[k+1] = vs[k];
                }
                strings[5] = strings[5] + (e + j * 1000000);

            });
            long l = System.currentTimeMillis();
            schema.consumeSourceFeed("LME", header.split("\\|"), bodys);
            System.out.println((System.currentTimeMillis() - l));



        }
        schema.PrintResult("77775");


    }
}
