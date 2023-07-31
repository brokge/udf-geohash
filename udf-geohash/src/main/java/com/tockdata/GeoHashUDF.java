package com.tockdata;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author chenlw
 * @date 2023/7/29 16:01
 */
@Description(
        name = "geohash",
        value = "_FUNC_(double lat,double lon,int precision(default 12)) - Returns geohash",
        extended = "Example:\n  > SELECT _FUNC_(\'20.1,111.2,6\');"
)
public class GeoHashUDF extends UDF {
    public String evaluate(Double lat, Double lon, Integer precision) {
        if (precision > 12) {
            throw new IllegalArgumentException("A geohash can only be 12 character long.");
        } else if (Math.abs(lat) <= 90.0D && Math.abs(lon) <= 180.0D) {
            return GeoHash.geoHashStringWithCharacterPrecision(lat, lon, precision);
        } else {
            throw new IllegalArgumentException("Can't have lat/lon values out of (-90,90)/(-180/180)");
        }

    }

    public String evaluate(Double lat, Double lon) {
        if (Math.abs(lat) <= 90.0D && Math.abs(lon) <= 180.0D) {
            return GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 12);
        } else {
            throw new IllegalArgumentException("Can't have lat/lon values out of (-90,90)/(-180/180)");
        }

    }

    public static void main(String[] args) {
        System.out.println(new GeoHashUDF().evaluate(39.971321, 116.490295));
        System.out.println(new GeoHashUDF().evaluate(39.971321, 116.490295, 9));
    }
}
