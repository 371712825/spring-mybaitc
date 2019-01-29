package spark;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class BxkOrderGoodsExtractor {
	
	public void extractor(SparkSession spark, String... args) throws Exception {

		spark.sql(this.getBxkOrderGoodsInfoSqlNew()).write().mode(SaveMode.Overwrite).json(HdfsUtils.HOST + "/data/bispark/bxkEx");
	}
	
	private String getBxkOrderGoodsInfoSqlNew() {
		StringBuilder sql = new StringBuilder();
		// 用户相关信息
		sql.append(" SELECT ");
		sql.append(" 	oi.user_id, ");
		sql.append(" 	oi.uid, ");
		sql.append(" 	oi.city_name, ");
		sql.append(" 	oi.province_name, ");
		sql.append(" 	'' AS bb_status, ");
		
		// 订单内容相关信息
		sql.append(" 	oi.pay_status, ");
		sql.append(" 	oi.order_id, ");  // 订单ID
		sql.append(" 	oi.pay_time AS `timestamp`, ");
		sql.append(" 	oi.add_time , ");
		sql.append(" 	oi.goods_amount, ");
		sql.append(" 	oi.money_paid, ");
		sql.append(" 	oi.order_sn, ");
		sql.append(" 	oi.order_id AS orderid_free, ");
		sql.append(" 	oi.order_id AS orderid_notfree, ");
		sql.append(" 	oi.order_id AS orderid_fm, ");
		sql.append(" 	oi.order_id AS orderid_inside, ");
		sql.append(" 	oi.order_id AS orderid_outside, ");
		sql.append(" 	oi.goods_amount AS amount_free, ");
		sql.append(" 	oi.goods_amount AS amount_notfree, ");
		
		// 订单来源相关信息
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN fm.fm_param                 ELSE wendafm.fm_param                 END fm_param, ");
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN fm.fm_description           ELSE wendafm.fm_description           END fm_description, ");
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN outside.outside_param       ELSE wendaoutside.outside_param       END outside_param, ");
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN outside.outside_description ELSE wendaoutside.outside_description END outside_description, ");
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN inside.inside_param         ELSE wendainside.inside_param         END inside_param, ");
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN inside.inside_description   ELSE wendainside.inside_description   END inside_description, ");
		
		// 商品相关属性
		sql.append(" 	s.name_cn `source`, ");
		sql.append(" 	og.goods_name, "); // 商品名称
		sql.append(" 	og.goods_id, "); // 商品ID
		sql.append(" 	og.goods_benefit, ");

		// 课程相关属性
		sql.append("	class_info.classid, "); // 课程ID
		sql.append("	class_info.class_type, "); // 课程类型 0-音频直播 2-音频录播
		sql.append("	class_info.type, "); // 课程类型 直播室类型 1-专家在线 2-专家微课堂
		sql.append("	class_info.sort_title, "); // 分类名称
		sql.append("	class_info.class_sort_id, "); // 课程分类ID
		sql.append("	class_info.`subject`, "); // 课程主题
		sql.append("	class_info.tag_name, "); // 标签名称
		sql.append("	class_info.mt_tag_id, "); // 标签ID
		sql.append("	class_info.mc_createat, "); // 课程创建时间
		sql.append("	class_info.mc_startat, "); // 课程开始时间
		sql.append("	class_info.mc_endat, "); // 课程结束时间
		sql.append("	class_info.class_duration, "); // 课程时长
		sql.append("	class_info.charge_type, "); // 课程收费类型：1-免费，2-付费

		// 听课人数相关信息
		sql.append("	learn_record.last_start_time, "); // 最后开始听课时间
		sql.append("	learn_record.last_end_time, "); // 最后结束听课时间
		sql.append("	learn_record.user_count, "); // 课程的听课人数
		
		// 问答相关信息
		//sql.append("	CASE WHEN ( mco.dateline>1537944328 and aqo.order_sn IS NULL ) THEN mai_p.detail         ELSE ''                  END detail, "); // 提问内容
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.ask_count      ELSE apq.ask_count       END ask_count, "); // 提问次数（含追问）
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.presenter_uid  ELSE apq.expert_uid      END presenter_uid, "); // 讲师UID
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.source_project ELSE aqo.source_project  END source_project, "); // 来源项目1-必修课 2-孕育问答
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.`status`       ELSE apq.status          END status, "); // 问答状态
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.dateline       ELSE apq.dateline        END dateline, "); // 创建时间
		sql.append(" 	CASE WHEN ( oi.app_name = 'bxk' ) THEN mai_p.reply_time     ELSE apq.last_reply_time END reply_time, "); // 最后回复时间
		sql.append(" 	mai_tmp.reply_time AS first_reply_time, "); // 最后回复时间均值
		sql.append(" 	mai_tmp.dateline AS first_dateline, "); 
		sql.append(" 	mai_tmp.count_parent_id, "); // 回复数总和 
		
		// 讲师相关信息
		sql.append("	mal.dateline AS teacher_dateline, "); // 讲师添加时间
		sql.append("	mal.live_room_id AS lecturer_live_room_id, "); // 讲师表记录的栏目(直播间)ID
		sql.append("	mal.status AS lecturer_status, "); // 讲师上下线状态：1-上线，0-下线
		sql.append("	mal.update_time AS lecturer_update_time, "); // 讲师更新时间
		
		// 必修课订单信息表中字段 0-单节课订单、1-约课订单、2-专栏订单、3-付费问答订单
		sql.append("	CASE WHEN ( oi.app_name = 'bxk' ) THEN mco.order_type WHEN ( aqo.order_type = 1 ) THEN 7 WHEN ( aqo.order_type = 2 ) THEN 8 ELSE 9 END order_type, ");
		sql.append("    oi.app_name AS detail, ");
		// 订单商品信息表 订单细分类别 11-必修课系列课，12-必修课付费问
		sql.append("	og.goods_own_type, ");
		sql.append("	og.purchase_price, "); // 进货价

		// 栏目(直播间)相关信息
		sql.append("	living_room.id AS live_room_id, ");
		sql.append("	living_room.title AS live_room_title, ");
		sql.append("	living_room.username AS live_room_username, ");
		sql.append("	living_room.dateline AS live_room_dateline, ");
		sql.append("	living_room.type AS live_room_type, ");
		sql.append("	living_room.is_open_ask AS live_room_isopenask, ");
		
		sql.append(" 	1 AS is_delete");
		
		// from的相关表
		sql.append(" FROM ");
		sql.append(" 	bi_mall_order_info AS oi ");
		sql.append(" 	LEFT JOIN bi_mall_order_goods og ON og.order_id = oi.order_id ");
		sql.append(" 	LEFT JOIN bi_mall_source_param s ON s.NAME = oi.source  ");
		sql.append(" 	AND s.app_name = 'bxk' ");
		
		// left join fm_param的相关属性值
		//sql.append(" 	LEFT JOIN ( ");
		//sql.append(" 	SELECT ot.order_sn, ot.param_id, ot.`value` fm_param, tv.param_description AS fm_description  ");
		//sql.append(" 	FROM bi_mall_order_track ot LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value`  AND ot.param_id = tv.param_id  ");
		//sql.append(" 	WHERE ot.param_id = 19 AND LENGTH( ot.VALUE ) <= 3  ");
		//sql.append(" 	ORDER BY ot.order_sn  ");
		//sql.append(" 	) fm ON fm.order_sn = oi.order_sn ");

		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` fm_param, ");
		sql.append(" 			tv.param_description AS fm_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 19 ");
		sql.append(" 	AND LENGTH(ot. VALUE) <= 3 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) fm ON fm.order_sn = oi.order_sn ");
		//问答订单fm
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` fm_param, ");
		sql.append(" 			tv.param_description AS fm_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 28 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) wendafm ON wendafm.order_sn = oi.order_sn ");

		
		// left join outside_param的相关属性值
		//sql.append(" 	LEFT JOIN ( ");
		//sql.append(" 	SELECT ot.order_sn, ot.param_id, ot.`value` outside_param, tv.param_description AS outside_description  ");
		//sql.append(" 	FROM bi_mall_order_track ot LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` AND ot.param_id = tv.param_id  ");
		//sql.append(" 	WHERE ot.param_id = 20 AND LENGTH( ot.VALUE ) <= 3  ");
		//sql.append(" 	ORDER BY ot.order_sn  ");
		//sql.append(" 	) outside ON outside.order_sn = oi.order_sn ");

		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` outside_param, ");
		sql.append(" 			tv.param_description AS outside_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 20 ");
		sql.append(" 	AND LENGTH(ot. VALUE) <= 3 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) outside ON outside.order_sn = oi.order_sn ");
		//问答订单outside
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` outside_param, ");
		sql.append(" 			tv.param_description AS outside_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 29 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) wendaoutside ON wendaoutside.order_sn = oi.order_sn ");

		
		// left join inside_param的相关属性值
		//sql.append(" 	LEFT JOIN ( ");
		//sql.append(" 	SELECT ot.order_sn, ot.param_id, ot.`value` inside_param, tv.param_description AS inside_description  ");
		//sql.append(" 	FROM bi_mall_order_track ot LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` AND ot.param_id = tv.param_id  ");
		//sql.append(" 	WHERE ot.param_id = 21 AND LENGTH( ot.VALUE ) <= 3  ");
		//sql.append(" 	ORDER BY ot.order_sn  ");
		//sql.append(" 	) inside ON inside.order_sn = oi.order_sn  ");

		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` inside_param, ");
		sql.append(" 			tv.param_description AS inside_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 21 ");
		sql.append(" 	AND LENGTH(ot. VALUE) <= 3 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) inside ON inside.order_sn = oi.order_sn ");
		//问答订单inside
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			ot.order_sn, ");
		sql.append(" 			ot.param_id, ");
		sql.append(" 			ot.`value` inside_param, ");
		sql.append(" 			tv.param_description AS inside_description ");
		sql.append(" 	FROM ");
		sql.append(" 		bi_mall_order_track ot ");
		sql.append(" 	LEFT JOIN bi_mall_track_value tv ON ot.`value` = tv.`param_value` ");
		sql.append(" 	AND ot.param_id = tv.param_id ");
		sql.append(" 	WHERE ");
		sql.append(" 		ot.param_id = 30 ");
		sql.append(" 	ORDER BY ");
		sql.append(" 		ot.order_sn ");
		sql.append(" 	) wendainside ON wendainside.order_sn = oi.order_sn ");
		
		// 关联必修课独立订单信息
		sql.append(" 	LEFT JOIN bi_mc_class_order mco ON mco.order_sn = oi.order_sn ");
		// 关联付费问答订单
		sql.append("    LEFT JOIN bi_ask_question_order aqo ON aqo.order_sn = oi.order_sn ");
		
		// 关联必修课单独订单商品表信息
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			mc.goods_id, ");
		sql.append(" 			mc.type, ");
		sql.append(" 			mc.class_sort_id, ");
		sql.append(" 			mc.classid, ");
		sql.append(" 			mc.`subject`, ");
		sql.append("			mc.class_type, ");
		sql.append("			mc.createat AS mc_createat, ");
		sql.append("			mc.startat AS mc_startat, ");
		sql.append("			mc.endat AS mc_endat, ");
		sql.append("			mc.class_duration, ");
		sql.append("			mc.charge_type, "); // 课程收费类型：1-免费，2-付费
		sql.append("			mc.live_room_id, "); // 直播间ID
		sql.append(" 			mcs.title AS sort_title, ");
		sql.append(" 			mt.tag_name, ");
		sql.append(" 			mt.id AS mt_tag_id  ");
		sql.append(" 		FROM ");
		sql.append(" 			bi_mc_class mc ");
		sql.append(" 			LEFT JOIN bi_mc_tag_class_relation mtc ON mtc.classid = mc.classid AND mtc.class_type = mc.class_type ");
		sql.append(" 			LEFT JOIN bi_mc_tag mt ON mt.id = mtc.id ");
		sql.append(" 			LEFT JOIN bi_mc_class_sort mcs ON mcs.id = mc.class_sort_id ");
		sql.append(" 	) class_info ON class_info.classid = mco.classid ");

		// 关联栏目(直播间)相关信息
		sql.append("	LEFT JOIN bi_mc_live_room living_room ON living_room.id = class_info.live_room_id ");
		
		// 关联教师的个人信息
		sql.append(" 	LEFT JOIN bi_mc_ask_lecturer mal ON mal.id = mco.goods_id ");
		
		// 关联提问信息详情
		sql.append(" 	LEFT JOIN bi_mc_ask_info mai_p ON mai_p.order_sn = mco.order_sn");
		sql.append("    LEFT JOIN bi_ask_paid_question apq ON apq.order_sn = aqo.order_sn ");
		
		// 关联追问情况信息
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			mai.parent_id, ");
		sql.append(" 			collect_set(mai.dateline)[0] dateline, ");
		sql.append(" 			collect_set(mai.reply_time)[0] reply_time, ");
		sql.append(" 			count( mai.parent_id ) count_parent_id  ");
		sql.append(" 		FROM ");
		sql.append(" 			 bi_mc_ask_info mai  ");
		sql.append(" 		WHERE ");
		sql.append(" 			mai.parent_id != 0  ");
		sql.append(" 		GROUP BY ");
		sql.append(" 		mai.parent_id  ");
		sql.append(" 	) mai_tmp ON mai_tmp.parent_id = mai_p.id ");
		
		// 关联实际听课人数相关信息
		sql.append(" 	LEFT JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append(" 			mld.classid, ");
		sql.append(" 			1 AS user_count, ");
		sql.append(" 			mld.last_start_time, ");
		sql.append(" 			mld.uid, ");
		sql.append(" 			mld.last_end_time ");
		sql.append(" 		FROM ");
		sql.append(" 			bi_mc_learning_duration mld  ");
		sql.append(" 	) learn_record ON learn_record.classid = class_info.classid AND learn_record.uid = mco.uid  ");

		// 关联栏目(直播间)相关信息
		//sql.append(" 	LEFT JOIN ( ");
		//sql.append(" 		SELECT ");
		//sql.append(" 			mlv.id, ");
		//sql.append(" 			mlv.title, ");
		//sql.append(" 			mlv.username, ");
		//sql.append(" 			mlv.dateline, ");
		//sql.append(" 			mlv.type, ");
		//sql.append(" 			mlv.is_open_ask, ");
		//sql.append(" 			mc.classid ");
		//sql.append(" 		FROM ");
		//sql.append(" 			bi_mc_live_room mlv  ");
		//sql.append(" 		LEFT JOIN bi_mc_class mc ON mc.live_room_id = mlv.id  ");
		//sql.append(" 	) living_room ON living_room.classid = mco.classid  ");

		// 筛选条件
		sql.append(" WHERE oi.app_name = 'bxk' OR oi.app_name='wenda' ");
		sql.append(" ORDER BY oi.order_sn ");
		return sql.toString();
	}
	public static void main(String[] args) {
		StringBuilder sql = new StringBuilder();

		System.out.println(sql.toString());
	}
}
