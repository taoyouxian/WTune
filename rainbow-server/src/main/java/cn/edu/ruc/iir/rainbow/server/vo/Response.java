package cn.edu.ruc.iir.rainbow.server.vo;

import java.io.Serializable;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.server.vo
 * @ClassName: Response
 * @Description: return object
 * @author: tao
 * @date: Create in 2019-12-13 10:51
 **/
public class Response<T> implements Serializable {

    private static final long serialVersionUID = 4402407305608555564L;
    private static final int SUCCESS_CODE = 0;

    private int errCode;
    private String errMsg;
    private int code;
    private String msg;
    private T data;

    public static Response<Object> buildSucResp(Object data) {
        Response<Object> resp = new Response<>();
        resp.setErrCode(SUCCESS_CODE);
        resp.setMsg("SUCCESS");
        resp.setData(data);
        return resp;
    }

    public static Response<Object> buildSucResp(Object data, String errorMsg) {
        Response<Object> resp = new Response<>();
        resp.setErrCode(SUCCESS_CODE);
        resp.setMsg(errorMsg);
        resp.setData(data);
        return resp;
    }

    public static Response<Object> buildFailureResp(int code, String errorMsg) {
        Response<Object> resp = new Response<>();
        resp.setErrCode(code);
        resp.setMsg(errorMsg);
        return resp;
    }

    public static Response<Object> buildFailureResp(int code, String errorMsg, String ext) {
        Response<Object> resp = new Response<>();
        resp.setErrCode(code);
        resp.setMsg(errorMsg);
        resp.setData(ext);
        return resp;
    }

    public int getErrCode() {
        return errCode;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Response{" +
                "errCode=" + errCode +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                '}';
    }
}
