package XiaoTest.Xiaodai.bo;

import java.io.Serializable;
import java.sql.Date;


/** 
* @author Lusx 
* @date 2019年5月13日 下午2:19:18 
*/
public class HJsonBo implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2012536676135598060L;

	private String remote_addr;
	
	private String uripath;
	
	private Date timestamp;

	public HJsonBo() {}
	
	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getUripath() {
		return uripath;
	}

	public void setUripath(String uripath) {
		this.uripath = uripath;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}


}
