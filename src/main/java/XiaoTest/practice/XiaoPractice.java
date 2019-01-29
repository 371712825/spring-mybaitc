package XiaoTest.practice;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.execution.vectorized.ColumnVector.Array;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import XiaoTest.Xiaodai.bo.ModelBo;
import enumUse.City;

public class XiaoPractice {

	
	public static void main(String[] args) {
		
		String str = "{\"name\":\"pihy\",\"uid\":\"\",\"orgId\":\"115\",\"address\":\"chinamususmen13\"}";
		//ModelBo mb = JSON.toJavaObject(JSON.parseObject(str), ModelBo.class);
		ModelBo mb = new ModelBo();
		
		System.out.println(mb.getUid()==null);
		
		/*JSONObject analysedRecord = new JSONObject();
		analysedRecord.put("province", "中国省");
		analysedRecord.put("city", "bbb");
		
		
		if (analysedRecord.containsKey("province") && StringUtils.isNotBlank(analysedRecord.getString("province"))) {
			if (StringUtils.isBlank(analysedRecord.getString("city"))) {
				analysedRecord.put("city", City.getCityByProvince(analysedRecord.getString("province")));
			} else if ("中国省".equals(analysedRecord.getString("province"))) {
				analysedRecord.put("province", City.BJ.getProvince());
				analysedRecord.put("city", City.BJ.getCity());
			}
			
		} else {
			analysedRecord.put("province", City.BJ.getProvince());
			analysedRecord.put("city", City.BJ.getCity());
		}
		
		System.out.println(analysedRecord.toJSONString());*/
		
		//MsgPackUtils.getDeserializedJSONObject
		
		//String data = "{\"cubeId\":\"63\",\"chartType\":\"table\",\"chartOption\":[],\"cluster\":\"myDruid\",\"params\":{\"dataSource\":\"dmp-receive-userAnalysis\",\"dimensions\":[{\"column\":\"position\",\"limit\":\"10000\",\"mergeConfig\":\"{\\\"SEARCH_REC_ALL_MID\\\":\\\"搜索中部推荐\\\",\\\"SEARCH_FLOW_THREAD\\\":\\\"搜索帖子信息流\\\",\\\"SEARCH_FLOW_USER\\\":\\\"搜索用户列表\\\",\\\"HOTSPOT_REC_MID\\\":\\\"热点中部推荐\\\",\\\"HOTSPOT_FLOW\\\":\\\"热点帖子信息流\\\",\\\"HOTTOPIC_FLOW\\\":\\\"热议帖子信息流\\\",\\\"THREAD_REC_DETAIL_MID\\\":\\\"帖子详情页推荐\\\",\\\"TOOL_FLOW_EIGHTGRID\\\":\\\"首页八宫格\\\",\\\"TOOL_FLOW_GROUP\\\":\\\"圈子胶囊区\\\",\\\"TOOL_FOUND_TOOLLIST\\\":\\\"发现页工具\\\",\\\"TOOL_FOUND_SERVICELIST\\\":\\\"发现页服务\\\",\\\"TOOL_FLOW_TOOLLIST\\\":\\\"工具列表页\\\",\\\"TOOL_FLOW_SERVICELIST\\\":\\\"服务列表页\\\",\\\"SHARE_FATHER\\\":\\\"分享给爸爸\\\",\\\"SHARE_WEIXINFEED\\\":\\\"分享到微信朋友圈\\\",\\\"SHARE_WEIXIN\\\":\\\"分享到微信好友\\\",\\\"SHARE_QQ\\\":\\\"分享到QQ\\\",\\\"SHARE_WEIBO\\\":\\\"分享到微博\\\",\\\"SHARE_MOTHER\\\":\\\"分享给妈妈\\\",\\\"HOTTHREAD_REC\\\":\\\"首页精选个性化推荐帖子\\\",\\\"HOTTHREAD_FLOW\\\":\\\"首页精选帖子信息流（运营推送）\\\",\\\"HEADER_POINT_BOOKSTORE\\\":\\\"91baby头部书城入口\\\",\\\"THREAD_REC_BOTTOM_1\\\":\\\"91baby帖子底部推荐位1\\\",\\\"THREAD_REC_BOTTOM_2\\\":\\\"91baby帖子底部推荐位2\\\",\\\"THREAD_REC_BOTTOM_3\\\":\\\"91baby帖子底部推荐位3\\\",\\\"THREAD_REC_BOTTOM_4\\\":\\\"91baby帖子底部推荐位4\\\",\\\"THREAD_REC_BOTTOM_5\\\":\\\"91baby帖子底部推荐位5\\\",\\\"THREAD_REC_BOTTOM_6\\\":\\\"91baby帖子底部推荐位6\\\",\\\"THREAD_REC_BOTTOM_7\\\":\\\"91baby帖子底部推荐位7\\\",\\\"THREAD_REC_BOTTOM_8\\\":\\\"91baby帖子底部推荐位8\\\",\\\"THREAD_REC_BOTTOM_9\\\":\\\"91baby帖子底部推荐位9\\\",\\\"THREAD_REC_BOTTOM_10\\\":\\\"91baby帖子底部推荐位10\\\",\\\"THREAD_REC_BOTTOM_11\\\":\\\"91baby帖子底部推荐位11\\\",\\\"THREAD_REC_BOTTOM_12\\\":\\\"91baby帖子底部推荐位12\\\",\\\"GLOBALPOPUP_REC\\\":\\\"91baby全局弹框\\\",\\\"FORUM_FLOW_1\\\":\\\"91baby板块-帖子列表1\\\",\\\"FORUM_FLOW_2\\\":\\\"91baby板块-帖子列表2\\\",\\\"FORUM_FLOW_3\\\":\\\"91baby板块-帖子列表3\\\",\\\"FORUM_FLOW_4\\\":\\\"91baby板块-帖子列表4\\\",\\\"FORUM_FLOW_5\\\":\\\"91baby板块-帖子列表5\\\",\\\"HOTTHREAD_CITYNEWS\\\":\\\"首页精选信息流同城资讯\\\",\\\"HOTTHREAD_REWARD\\\":\\\"首页精选信息流悬赏\\\",\\\"HOTTHREAD_TRY\\\":\\\"首页精选信息流试用\\\",\\\"HOTTHREAD_KNOWLEDGE\\\":\\\"首页精选信息流知识\\\",\\\"HOTTHREAD_SUBJECT\\\":\\\"首页精选信息流专题\\\",\\\"HOTTHREAD_TIMEONPAGE_SINGLESTARTUP\\\":\\\"单次启动首页精选 Tab 停留时长\\\",\\\"OVERALL_STARTUP\\\":\\\"轻聊启动 APP\\\",\\\"CSERVICE_NAMING\\\":\\\"PT-起名页面\\\",\\\"CSERVICE_NAMING_MID_BUTTON\\\":\\\"PT-起名页下单按钮\\\",\\\"CSERVICE_NAMINGPAY\\\":\\\"PT-起名支付页\\\",\\\"WXSA_TRY_HOMEPAGE\\\":\\\"试用-首页\\\",\\\"WXSA_TRY_HOMEPAGE_RIGHT_BUTTON\\\":\\\"试用-首页申请试用按钮\\\",\\\"WXSA_TRY_DETAIL\\\":\\\"试用-申请试用详情页\\\",\\\"WXSA_TRY_DETAIL_BOTTOM_BUTTON\\\":\\\"试用-申请试用详情页试用按钮\\\",\\\"WXSA_TRY_GROUP\\\":\\\"试用-申请试用参团页\\\",\\\"WXSA_TRY_GROUP_MID_BUTTON\\\":\\\"试用-申请试用参团页邀请好友按钮页\\\"}\",\"type\":\"String\"},{\"column\":\"item_name\",\"limit\":\"1000\",\"type\":\"String\"}],\"aggregations\":[{\"precision\":\"default\",\"column\":\"pv_sum_1\",\"name\":\"曝光数(pv)\",\"description\":\"\",\"filterd\":\"{\\\"type\\\":\\\"and\\\",\\\"fields\\\":[{\\\"type\\\":\\\"selector\\\",\\\"dimension\\\":\\\"event\\\",\\\"value\\\":\\\"impression\\\"},{\\\"field\\\":{\\\"values\\\":[\\\"SHARE_FATHER\\\",\\\"SHARE_WEIXINFEED\\\",\\\"SHARE_WEIXIN\\\",\\\"SHARE_QQ\\\",\\\"SHARE_WEIBO\\\",\\\"SHARE_MOTHER\\\",\\\"_SHARECHANEL_\\\"],\\\"type\\\":\\\"in\\\",\\\"dimension\\\":\\\"position\\\"},\\\"type\\\":\\\"not\\\"}]}\",\"type\":\"longSum\"},{\"precision\":\"jinyifa\",\"column\":\"devOuid_countd_1\",\"name\":\"曝光人数(uv)\",\"description\":\"\",\"filterd\":\"{\\\"type\\\":\\\"and\\\",\\\"fields\\\":[{\\\"type\\\":\\\"selector\\\",\\\"dimension\\\":\\\"event\\\",\\\"value\\\":\\\"impression\\\"},{\\\"field\\\":{\\\"values\\\":[\\\"SHARE_FATHER\\\",\\\"SHARE_WEIXINFEED\\\",\\\"SHARE_WEIXIN\\\",\\\"SHARE_QQ\\\",\\\"SHARE_WEIBO\\\",\\\"SHARE_MOTHER\\\",\\\"_SHARECHANEL_\\\"],\\\"type\\\":\\\"in\\\",\\\"dimension\\\":\\\"position\\\"},\\\"type\\\":\\\"not\\\"}]}\",\"type\":\"hyperUnique\"},{\"column\":\"pv_sum_2\",\"name\":\"点击数(pv)\",\"description\":\"\",\"filterd\":\"{\\\"type\\\":\\\"selector\\\",\\\"dimension\\\":\\\"event\\\",\\\"value\\\":\\\"click\\\"}\",\"type\":\"longSum\"},{\"precision\":\"default\",\"column\":\"pv_sum_2 / pv_sum_1 * 100\",\"name\":\"点击率(%)\",\"description\":\"点击数(pv) / 曝光数(pv) * 100\",\"filterd\":\"\",\"type\":\"expressionPost\"},{\"precision\":\"default\",\"column\":\"pv_sum_2 / devOuid_countd_1\",\"name\":\"人均点击量\",\"description\":\"\",\"filterd\":\"\",\"type\":\"expressionPost\"}],\"filter\":{\"filters\":[{\"column\":\"__time\",\"name\":\"行为时间\",\"filterType\":\"relative-time\",\"value\":[\"lastest7Days\"]},{\"column\":\"user_mark_1\",\"join\":\"\",\"mergeConfig\":\"\",\"type\":\"String\",\"filterType\":\"exclude\",\"value\":[\"null\"],\"sql\":\"\",\"addToContent\":false},{\"column\":\"position\",\"mergeConfig\":\"{\\\"SEARCH_REC_ALL_MID\\\":\\\"搜索中部推荐\\\",\\\"SEARCH_FLOW_THREAD\\\":\\\"搜索帖子信息流\\\",\\\"SEARCH_FLOW_USER\\\":\\\"搜索用户列表\\\",\\\"HOTSPOT_REC_MID\\\":\\\"热点中部推荐\\\",\\\"HOTSPOT_FLOW\\\":\\\"热点帖子信息流\\\",\\\"HOTTOPIC_FLOW\\\":\\\"热议帖子信息流\\\",\\\"THREAD_REC_DETAIL_MID\\\":\\\"帖子详情页推荐\\\",\\\"TOOL_FLOW_EIGHTGRID\\\":\\\"首页八宫格\\\",\\\"TOOL_FLOW_GROUP\\\":\\\"圈子胶囊区\\\",\\\"TOOL_FOUND_TOOLLIST\\\":\\\"发现页工具\\\",\\\"TOOL_FOUND_SERVICELIST\\\":\\\"发现页服务\\\",\\\"TOOL_FLOW_TOOLLIST\\\":\\\"工具列表页\\\",\\\"TOOL_FLOW_SERVICELIST\\\":\\\"服务列表页\\\",\\\"SHARE_FATHER\\\":\\\"分享给爸爸\\\",\\\"SHARE_WEIXINFEED\\\":\\\"分享到微信朋友圈\\\",\\\"SHARE_WEIXIN\\\":\\\"分享到微信好友\\\",\\\"SHARE_QQ\\\":\\\"分享到QQ\\\",\\\"SHARE_WEIBO\\\":\\\"分享到微博\\\",\\\"SHARE_MOTHER\\\":\\\"分享给妈妈\\\",\\\"HOTTHREAD_REC\\\":\\\"首页精选个性化推荐帖子\\\",\\\"HOTTHREAD_FLOW\\\":\\\"首页精选帖子信息流（运营推送）\\\",\\\"HEADER_POINT_BOOKSTORE\\\":\\\"91baby头部书城入口\\\",\\\"THREAD_REC_BOTTOM_1\\\":\\\"91baby帖子底部推荐位1\\\",\\\"THREAD_REC_BOTTOM_2\\\":\\\"91baby帖子底部推荐位2\\\",\\\"THREAD_REC_BOTTOM_3\\\":\\\"91baby帖子底部推荐位3\\\",\\\"THREAD_REC_BOTTOM_4\\\":\\\"91baby帖子底部推荐位4\\\",\\\"THREAD_REC_BOTTOM_5\\\":\\\"91baby帖子底部推荐位5\\\",\\\"THREAD_REC_BOTTOM_6\\\":\\\"91baby帖子底部推荐位6\\\",\\\"THREAD_REC_BOTTOM_7\\\":\\\"91baby帖子底部推荐位7\\\",\\\"THREAD_REC_BOTTOM_8\\\":\\\"91baby帖子底部推荐位8\\\",\\\"THREAD_REC_BOTTOM_9\\\":\\\"91baby帖子底部推荐位9\\\",\\\"THREAD_REC_BOTTOM_10\\\":\\\"91baby帖子底部推荐位10\\\",\\\"THREAD_REC_BOTTOM_11\\\":\\\"91baby帖子底部推荐位11\\\",\\\"THREAD_REC_BOTTOM_12\\\":\\\"91baby帖子底部推荐位12\\\",\\\"GLOBALPOPUP_REC\\\":\\\"91baby全局弹框\\\",\\\"FORUM_FLOW_1\\\":\\\"91baby板块-帖子列表1\\\",\\\"FORUM_FLOW_2\\\":\\\"91baby板块-帖子列表2\\\",\\\"FORUM_FLOW_3\\\":\\\"91baby板块-帖子列表3\\\",\\\"FORUM_FLOW_4\\\":\\\"91baby板块-帖子列表4\\\",\\\"FORUM_FLOW_5\\\":\\\"91baby板块-帖子列表5\\\",\\\"HOTTHREAD_CITYNEWS\\\":\\\"首页精选信息流同城资讯\\\",\\\"HOTTHREAD_REWARD\\\":\\\"首页精选信息流悬赏\\\",\\\"HOTTHREAD_TRY\\\":\\\"首页精选信息流试用\\\",\\\"HOTTHREAD_KNOWLEDGE\\\":\\\"首页精选信息流知识\\\",\\\"HOTTHREAD_SUBJECT\\\":\\\"首页精选信息流专题\\\",\\\"HOTTHREAD_TIMEONPAGE_SINGLESTARTUP\\\":\\\"单次启动首页精选 Tab 停留时长\\\",\\\"OVERALL_STARTUP\\\":\\\"轻聊启动 APP\\\",\\\"CSERVICE_NAMING\\\":\\\"PT-起名页面\\\",\\\"CSERVICE_NAMING_MID_BUTTON\\\":\\\"PT-起名页下单按钮\\\",\\\"CSERVICE_NAMINGPAY\\\":\\\"PT-起名支付页\\\",\\\"WXSA_TRY_HOMEPAGE\\\":\\\"试用-首页\\\",\\\"WXSA_TRY_HOMEPAGE_RIGHT_BUTTON\\\":\\\"试用-首页申请试用按钮\\\",\\\"WXSA_TRY_DETAIL\\\":\\\"试用-申请试用详情页\\\",\\\"WXSA_TRY_DETAIL_BOTTOM_BUTTON\\\":\\\"试用-申请试用详情页试用按钮\\\",\\\"WXSA_TRY_GROUP\\\":\\\"试用-申请试用参团页\\\",\\\"WXSA_TRY_GROUP_MID_BUTTON\\\":\\\"试用-申请试用参团页邀请好友按钮页\\\"}\",\"type\":\"String\",\"filterType\":\"include\",\"value\":[\"首页精选帖子信息流（运营推送）\"],\"addToContent\":false},{\"column\":\"item_name\",\"type\":\"String\",\"filterType\":\"exclude\",\"value\":[\"未录入\"],\"addToContent\":false}]},\"limitSpec\":{\"columns\":[{\"dimension\":\"pv_sum_1\",\"direction\":\"descending\"},{\"dimension\":\"pv_sum_1\",\"direction\":\"descending\"}]}}}";
		//JSONObject json = JSONObject.parseObject(data);
		//reSetJson(json);
		
		//System.out.println(json);
		
		/*int j = -1;
		label:while(true){
			System.out.println("1");
	        //我是第一层循环
	        for(;;) {
	            //我是第二层循环
	        	if (j<0) {
	        		break label; // continue label
	        	} else {
	        		break;
	        	}
	        }
	        System.out.println("2");
	        break;
	   } */
	}
	
	public static void reSetJson(JSONObject json) {
		json.put("ccc", "11111");
		
		System.out.println(json.get("cubeId"));
	}
}
