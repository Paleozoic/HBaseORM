package com.maxplus1.hd_client.hbase.type.rtn_row;

import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnTypeMetaData;
import com.maxplus1.hd_client.hbase.cache.TableColumnCacheManager;
import com.maxplus1.hd_client.hbase.operations.client.spring_hbase.beans.ColumnMetaData;
import com.maxplus1.hd_client.hbase.operations.client.rtn_map.beans.TableColumn;
import com.maxplus1.hd_client.hbase.type.resolvers.TypeResolver;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by qxloo on 2016/11/27.
 */
@Deprecated
public class RowResolver {

    public static Object buildCellResult(String tableName, String family, String column, Cell cell){
        ColumnMetaData columnMetaData = new ColumnMetaData(tableName,family,column);
        ColumnTypeMetaData.ColumnType columnType = TableColumnCacheManager.get(columnMetaData);
        return buildCellResult(columnType,cell);
    }

    /**
     * 默认是String类型
     * @param columnType
     * @param cell
     * @return
     */
    public static Object buildCellResult(ColumnTypeMetaData.ColumnType columnType, Cell cell){
//        String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
//        String column = Bytes.toString(CellUtil.cloneQualifier(cell));
        byte[] value = CellUtil.cloneValue(cell);
//        long timestamp = cell.getTimestamp();
        if(columnType!=null){
            TypeResolver resolver = TypeResolverFactory.getResolver(columnType);
            return resolver.toObject(value);
        }else{//默认string
            return Bytes.toString(value);
        }
    }

    /**
     * 默认转String类型
     * @param columnType
     * @param value
     * @return
     */
    public static byte[] toBytes(ColumnTypeMetaData.ColumnType columnType, Object value){
        if(columnType!=null){
            TypeResolver resolver = TypeResolverFactory.getResolver(columnType);
            return resolver.toBytes(value);
        }else{
            return Bytes.toBytes(value+"");
        }
    }
}
