package XiaoTest.Xiaodai.bo;

import java.util.List;


/**
 * 公共返回结果包装类
 * @author Tommy Lau
 */
public class Result {

	private Integer code = 200;
	private String msg = "success";
	private Object data;

	private Integer total;
	private List<?> rows;
	
	public Result() {}
	
	public Result(Object data) {
		this.data = data;
	}
	
	public Result(Integer code,String msg) {
		this.code = code;
		this.msg = msg;
	}
	
	public Result(Integer total, List<?> rows) {
		this.total = total;
		this.rows = rows;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public List<?> getRows() {
		return rows;
	}

	public void setRows(List<?> rows) {
		this.rows = rows;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}
}
