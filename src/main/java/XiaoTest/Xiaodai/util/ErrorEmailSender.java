package XiaoTest.Xiaodai.util;

import ch.qos.logback.classic.net.SMTPAppender;

public class ErrorEmailSender extends SMTPAppender{
	private String environment = System.getenv("ENVIRONMENT_MAIN");
	
	@Override
	public void setSubject(String subject) {
		if("product".equals(environment)){
			super.setSubject("【ERROR】: 线上");
		}else if("grey".equals(environment)){
			super.setSubject("【ERROR】: 灰度");
		}
	}
	
	@Override
	public void start() {
		if("product".equals(environment) || "grey".equals(environment)){
			super.start();
		} else {
			super.stop();
		}
	}
}