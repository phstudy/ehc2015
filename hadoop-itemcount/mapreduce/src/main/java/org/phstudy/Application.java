package org.phstudy;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

/**
 * Created by study on 4/29/15.
 */
public class Application {
    static int PRODUCT_IDX = 0;
    static int NUMBER_IDX = 1;
    static int PRICE_IDX = 2;

    public static void main(String[] args) throws Exception {
        GZIPInputStream gis = new GZIPInputStream(new FileInputStream(new File("./EHC_1st.tar.gz")), 65535);
        //FileInputStream fis = new FileInputStream(new File("./EHC_1st_round.log"));
        InputStreamReader isr = new InputStreamReader(gis);
        BufferedReader br = new BufferedReader(isr);

        Map<String, Double> map = new HashMap<>();

        String line;
        while ((line = br.readLine()) != null) {
            int actIdx = line.indexOf("act=order");
            if (actIdx > 0) {
                int plistIdx = line.indexOf("plist=", actIdx);
                int semiIdx = line.indexOf(";", plistIdx);

                String plist = line.substring(plistIdx + "plist=".length(), semiIdx);
                String products[] = plist.split(",");
                if (products.length == 1) {
                    //System.out.println(line);
                } else {
                    for (int i = 0; i < products.length; i += 3) {
                        int number = Integer.parseInt(products[i + NUMBER_IDX]);
                        double price = Double.parseDouble(products[i + PRICE_IDX]);
                        Double subTotal = number * price;

                        map.computeIfPresent(products[i + PRODUCT_IDX], (key, value) -> value + subTotal);
                        map.computeIfAbsent(products[i + PRODUCT_IDX], (key) -> subTotal);
                    }
                }
            }
        }
        Map sortMap = SortByValue(map);
        int [] cnt = new int[]{0};
        sortMap.forEach((key, value) -> {
            System.out.println(value + ", " + key);
            cnt[0]++;
            if(cnt[0] > 20) {
                System.exit(0);
            }
        });
    }

    public static TreeMap<String, Double> SortByValue
            (Map<String, Double> map) {
        ValueComparator vc = new ValueComparator(map);
        TreeMap<String, Double> sortedMap = new TreeMap<>(vc);
        sortedMap.putAll(map);
        return sortedMap;
    }

    static class ValueComparator implements Comparator<String> {

        Map<String, Double> map;

        public ValueComparator(Map<String, Double> base) {
            this.map = base;
        }

        public int compare(String a, String b) {
            if (map.get(a) >= map.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }
}
