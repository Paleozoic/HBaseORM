package com.maxplus1.hd_client.hbase.operations;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.type.TypeReference;

/**
 * TODO 以后如果需要针对特定rowkey设计分页，可将扩展PageInfo
 * 分页查询的结果对象
 * @author zachary.zhang
 * @author Paleo
 *
 */
@Getter
@Setter
@NoArgsConstructor
public class PageInfo<T> {

    private Class<T> poClass;
    /**
     * 返回查询之后获得的数据集
     */
    private List<T> dataSet;
    /**
     * 设定开始查询的startRow.第一次由用户提供，后面通过框架计算生成.
     */
    private byte[] startRow;

    /**
     * 设定结束查询的stopRow
     */
    private byte[] stopRow;
    /**
     * TODO: 跳页设计中
     * 上一次请求页码
     */
    private int prePageNo;
    /**
     * TODO: 跳页设计中
     * 是否是跳页
     */
    private boolean isJumpTo;
    /**
     * TODO: 跳页设计中
     * 上一次请求的开始行
     */
    private byte[] lastStartRow;
    /**
     * TODO: 跳页设计中
     * 上一次请求的终止行
     */
    private byte[] lastStopRow;
    /**
     * 当前结果集是第几页，方便页面显示.default是1页
     */
    private int currentPage = 1;

    /**
     * 每页显示多少行数据.default是100条数据
     * 注意：一经设置，中途查询数据的过程中，请勿修改
     */
    private int pageSize = 100;


    public void setStartRow(String startRow){
        this.startRow = Bytes.toBytes(startRow);
    }

    public void setStopRow(byte[] stopRow){
        this.stopRow = stopRow;
    }

    public void setStartRow(byte[] startRow){
        this.startRow = startRow;
    }

    public void setStopRow(String stopRow){
        this.stopRow = Bytes.toBytes(stopRow);
    }

    public String getStartRowAsString(){
        return Bytes.toString(startRow);
    }

    public String getStopRowAsString(){
        return Bytes.toString(stopRow);
    }

    public String getLastStartRowAsString(){
        return Bytes.toString(lastStartRow);
    }

    public boolean isPreviousPage(){
        return prePageNo - currentPage == 1;
    }

    public PageInfo(Class<T> poClass) {
        this.poClass = poClass;
    }

    /**
     * <Pre>
     * this is abstract class,the sub class should extends this class that we can get the generic type.
     * <p> just like use {@link TypeReference} in Jackson
     * 需要将PageInfo转为抽象类，然后子类（实现类）通过次方法获取T.class
     * get the generic type T class
     * @return Class
     */
    /*public Class<T> getGenericType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }*/

    // TODO should add totalCount in future
    // private int totalCount
}
