package com.maxplus1.hd_client.hbase.exception;

/**
 * Created by qxloo on 2016/12/26.
 */
public class HbaseClientException extends RuntimeException {

    public HbaseClientException() {
        super();
    }


    public HbaseClientException(String message) {
        super(message);
    }


    public HbaseClientException(String message, Throwable cause) {
        super(message, cause);
    }


    public HbaseClientException(Throwable cause) {
        super(cause);
    }
}
