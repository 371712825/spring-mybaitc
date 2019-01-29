package spark;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class BxkOrderGoodsTransform {

	public void transform(SparkSession spark, String... args) throws Exception{

		FSDataOutputStream output = HdfsUtils.getInstance().create(new Path("/data/bispark/bxkTf/data.json"), true);
		Dataset<Row> bxkExData = spark.read().json(HdfsUtils.HOST + "/data/bispark/bxkEx/*.json");
		
		final byte[] newLineBytes = "\n".getBytes("UTF-8");
		byte[] tempBytes = null;

		// 转换逻辑
		JSONObject json = null;
		JSONObject newRecord = null;
		Iterator<String> it = bxkExData.toJSON().toLocalIterator();
		while (it.hasNext()) {
			json = JSONObject.parseObject(it.next());
			newRecord = new JSONObject();
			
			// 设置创建时间维度列相关信息
			this.setCreateTime(newRecord, json);
			// 给渠道进行赋值
			this.setChannel(json);
			// 填充数据
			this.fillValue(newRecord, json);
			
			tempBytes = newRecord.toJSONString().getBytes("UTF-8");
			output.write(tempBytes, 0, tempBytes.length);
			output.write(newLineBytes, 0, newLineBytes.length);
		}

		output.flush();
		output.close();
	}
	
	private void fillValue(JSONObject newRecord, JSONObject json) {
		// 订单相关信息
		newRecord.put("uid", json.getString("uid"));
		newRecord.put("user_id", json.getString("user_id"));
		newRecord.put("city_name", this.emptyString(json.getString("city_name")));
		newRecord.put("province_name", this.emptyString(json.getString("province_name")));
		newRecord.put("bb_status", this.emptyString(json.getString("bb_status")));
		
		newRecord.put("pay_status", this.emptyString(json.getString("pay_status")));
		newRecord.put("order_id", json.getString("order_id"));
		newRecord.put("timestamp", json.getIntValue("timestamp"));
		newRecord.put("add_time", json.getIntValue("add_time"));
		newRecord.put("goods_amount", json.getDoubleValue("goods_amount"));
		newRecord.put("money_paid", json.getDoubleValue("money_paid"));
		newRecord.put("order_sn", json.getString("order_sn"));
		newRecord.put("orderid_free", json.getIntValue("orderid_free"));
		newRecord.put("orderid_notfree", json.getIntValue("orderid_notfree"));
		
		newRecord.put("orderid_fm", json.getIntValue("orderid_fm"));
		newRecord.put("orderid_inside", json.getIntValue("orderid_inside"));
		newRecord.put("orderid_outside", json.getIntValue("orderid_outside"));
		newRecord.put("fm_param", json.getString("fm_param"));
		newRecord.put("fm_description", json.getString("fm_description"));
		newRecord.put("outside_param", json.getString("outside_param"));
		newRecord.put("outside_description", json.getString("outside_description"));
		newRecord.put("inside_param", json.getString("inside_param"));
		newRecord.put("inside_description", json.getString("inside_description"));
		
		newRecord.put("source", this.emptyString(json.getString("source")));
		newRecord.put("goods_name", this.emptyString(json.getString("goods_name"))); // 课程名称、教师名称、专栏名称
		newRecord.put("goods_id", json.getIntValue("goods_id")); // 课程ID、教师ID
		newRecord.put("goods_benefit", json.getDoubleValue("goods_benefit"));

		// 课程相关信息
		newRecord.put("mc_class_classid", json.getIntValue("classid")); // 课程ID
		newRecord.put("class_type", this.emptyString(json.getString("class_type"))); // 课程类型 判空
		newRecord.put("type", json.getIntValue("type")); // 课程类型
		newRecord.put("class_sort_id", json.getIntValue("class_sort_id"));  // 课程分类
		newRecord.put("sort_title", this.emptyString(json.getString("sort_title"))); // 分类名称
		newRecord.put("subject", this.emptyString(json.getString("subject"))); // 课程主题
		newRecord.put("mt_tag_id", json.getIntValue("mt_tag_id")); // 标签ID
		newRecord.put("tag_name", this.emptyString(json.getString("tag_name"))); // 标签名称
		newRecord.put("mc_createat", json.getIntValue("mc_createat")); // 课程创建时间
		newRecord.put("mc_startat", json.getIntValue("mc_startat")); // 课程开始时间
		newRecord.put("mc_endat", json.getIntValue("mc_endat")); // 课程结束时间
		newRecord.put("class_duration", json.getIntValue("class_duration")); // 课程时长
		newRecord.put("mc_class_charge_type", json.getIntValue("charge_type")); // 课程收费类型：1-免费，2-付费

		// 设置 课程信息是否存在字段，0-不存在，1-存在
		if (json.getIntValue("classid") > 0) {
			newRecord.put("mc_class_exist", 1);
		} else {
			newRecord.put("mc_class_exist", 0);
		}

		newRecord.put("user_count", json.getIntValue("user_count")); // 某课程下提问人数总和
		newRecord.put("last_start_time", json.getIntValue("last_start_time")); // 开始听课时间
		newRecord.put("last_end_time", json.getIntValue("last_end_time")); // 结束听课时间
		int s = json.getIntValue("last_start_time");
		int e = json.getIntValue("last_end_time");
		newRecord.put("learn_time", e - s >= 0 ? ( e - s ) : 0); // 学习时间

		newRecord.put("detail", this.emptyString(json.getString("detail"))); // 提问细节
		newRecord.put("ask_count", json.getIntValue("ask_count")); // 提问次数	
		
		newRecord.put("presenter_uid", json.getIntValue("presenter_uid")); // 讲师ID
		newRecord.put("source_project", json.getIntValue("source_project")); // 来源项目 1-必修课 2-问答
		newRecord.put("status", json.getIntValue("status")); // 问答状态
		newRecord.put("dateline", json.getIntValue("dateline")); // 问答创建时间
		newRecord.put("reply_time", json.getIntValue("reply_time")); // 最后回复时间
		newRecord.put("first_dateline", json.getIntValue("first_dateline")); // 初次提问创建
		newRecord.put("first_reply_time", json.getIntValue("first_reply_time")); // 初次提问回复应答时间
		newRecord.put("count_parent_id", json.getIntValue("count_parent_id")); // 某课程下付费客户提问人数和
		
		newRecord.put("teacher_dateline", json.getIntValue("teacher_dateline")); // 讲师添加时间
		newRecord.put("lecturer_live_room_id", json.getIntValue("lecturer_live_room_id")); // 讲师表记录的栏目(直播间)ID
		newRecord.put("lecturer_status", json.getIntValue("lecturer_status")); // 讲师上下线状态：1-上线，0-下线
		newRecord.put("lecturer_update_time", json.getIntValue("lecturer_update_time")); // 讲师更新时间

		// 设置 讲师信息是否存在字段，0-不存在，1-存在
		if (json.getIntValue("presenter_uid") > 0) {
			newRecord.put("lecturer_exist", 1);
		} else {
			newRecord.put("lecturer_exist", 0);
		}

		newRecord.put("order_type", json.getIntValue("order_type")); // 订单类型 0-单节课订单、1-约课订单、2-专栏订单、3-付费问答订单
		newRecord.put("goods_own_type", json.getIntValue("goods_own_type")); // 订单商品信息表 订单细分类别 11-必修课系列课，12-必修课付费问
		newRecord.put("purchase_price", json.getDoubleValue("purchase_price")); // 进货价
		
		newRecord.put("is_delete", 1); // 数据清洗特殊预留字段
		newRecord.put("record_number", 1); // 数据清洗特殊预留字段
		
		newRecord.put("is_free", json.getString("is_free")); // 是否免费
		newRecord.put("is_discount", json.getString("is_discount")); // 是否使用优惠卷

		// 栏目(直播间)相关信息
		newRecord.put("live_room_id", json.getIntValue("live_room_id")); // 栏目(直播间)ID

		// 设置栏目名称
		if (StringUtils.isNotBlank(json.getString("live_room_title"))) {
			newRecord.put("live_room_title", json.getString("live_room_title"));
		} else {
			newRecord.put("live_room_title", this.emptyString(json.getString("live_room_username")));
		}

		newRecord.put("live_room_dateline", json.getIntValue("live_room_dateline")); // 栏目(直播间)创建时间
		newRecord.put("live_room_type", json.getIntValue("live_room_type")); // 栏目(直播间)类型：1个人，2-机构
		newRecord.put("live_room_isopenask", json.getIntValue("live_room_isopenask")); // 栏目(直播间)是否开通付费问答：1-是，0-否

		// 设置 栏目是否存在字段，0-不存在，1-存在
		if (json.getIntValue("live_room_id") > 0) {
			newRecord.put("live_room_exist", 1);
		} else {
			newRecord.put("live_room_exist", 0);
		}

		
		// 判断是否听完90%
		if (json.getIntValue("class_duration") == 0) {
			newRecord.put("is_finish", -1);
		} else {
			if (newRecord.getIntValue("learn_time") / json.getIntValue("class_duration") >= 0.9) {
				newRecord.put("is_finish", 1);
			} else {
				newRecord.put("is_finish", 0);
			}
		}
		
		// 回复数维度标识
		if (json.getIntValue("reply_time") != 0) {
			// 此时有回复时间，说明有回复后台发我一下，我上去看看
			newRecord.put("ask_count_reply", 1);
		} else {
			// 此时有回复时间，说明有回复
			newRecord.put("ask_count_reply", 0);
		}

		// 追问维度标识
		if (json.getIntValue("first_reply_time") != 0) {
			// 此时有回复时间，说明有回复
			newRecord.put("count_first_reply", 1);
		} else {
			// 此时有回复时间，说明有回复
			newRecord.put("count_first_reply", 0);
		}
	}
	
	// 设置创建时间
	private void setCreateTime(JSONObject newRecord, JSONObject json) {
		Integer create_time = 0;
		if (json.getIntValue("mc_createat") != 0) {
			create_time = json.getIntValue("mc_createat");
		} else if (json.getIntValue("teacher_dateline") != 0) {
			create_time = json.getIntValue("teacher_dateline");
		}
		newRecord.put("create_time", create_time);
	}
	
	// 设置订单相关的渠道参数
	private void setChannel(JSONObject json) {
		
		Boolean isUse = false;
		String fm_param = json.getString("fm_param");
		String outside_param = json.getString("outside_param");
		String inside_param = json.getString("inside_param");

		if (StringUtils.isBlank(fm_param)) {
			json.put("fm_param", "其他渠道类型");
			json.put("fm_description", "其他渠道类型");
		} else {
			isUse = true;
		}

		if (outside_param == null) {
			json.put("outside_param", "其他渠道类型");
			json.put("outside_description", "其他渠道类型");
		} else {
			isUse = true;
		}

		if (inside_param == null) {
			json.put("inside_param", "其他渠道类型");
			json.put("inside_description", "其他渠道类型");
		} else {
			isUse = true;
		}

		if (!isUse) {
			json.put("fm_param", "无跟踪记录");
			json.put("outside_param", "无跟踪记录");
			json.put("inside_param", "无跟踪记录");
			json.put("fm_description", "无跟踪记录");
			json.put("inside_description", "无跟踪记录");
			json.put("outside_description", "无跟踪记录");
		}

		if (StringUtils.isBlank(json.getString("inside_description"))) {
			json.put("inside_description", "未录入描述");
		}

		if (StringUtils.isBlank(json.getString("fm_description"))) {
			json.put("fm_description", "未录入描述");
		}

		if (StringUtils.isBlank(json.getString("outside_description"))) {
			json.put("outside_description", "未录入描述");
		}

		if (json.getInteger("money_paid") == 0) {
			json.put("is_free", "免费");
			json.remove("money_paid");
		} else {
			json.put("is_free", "付费");
			json.remove("money_paid");
		}

		if (json.getInteger("goods_benefit") > 0) {
			json.put("is_discount", "有优惠卷");
		} else {
			json.put("is_discount", "无优惠卷");
		}
	}
	
	// 空字符串置为0
	private String emptyString(String str) {
		if (StringUtils.isBlank(str)) {
			return "未录入";
		} else {
			return str;
		}
	}
}
