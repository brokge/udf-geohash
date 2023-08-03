package com.tockdata;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author chenlw
 * @date 2023/7/31 14:59
 */
@Description(
        name = "geohash",
        value = "_FUNC_(double lat,double lon,int precision(default 12)) - Returns geohash",
        extended = "Example:\n  > SELECT _FUNC_(\'20.1,111.2,6\');"
)
public class GeoHashV2UDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 3) {
            throw new UDFArgumentException("arguments size must be 3, FUNC_(double lat,double lon,int precision(max 12)) - Returns geohash");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        double lat = Double.parseDouble((String) deferredObjects[0].get());
        double lon = Double.parseDouble((String) deferredObjects[1].get());
        int precision = Integer.parseInt((String) deferredObjects[2].get());
        if (precision > 12) {
            throw new IllegalArgumentException("A geohash can only be 12 character long.");
        } else if (Math.abs(lat) <= 90.0D && Math.abs(lon) <= 180.0D) {
            return GeoHash.geoHashStringWithCharacterPrecision(lat, lon, precision);
        } else {
            throw new IllegalArgumentException("Can't have lat/lon values out of (-90,90)/(-180/180)");
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "_FUNC_(double lat,double lon,int precision(max 12)) - Returns geohash";
    }
}
