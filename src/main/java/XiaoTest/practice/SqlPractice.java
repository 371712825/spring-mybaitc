package XiaoTest.practice;
/** 
* @author Lusx 
* @date 2019年5月20日 下午2:31:00 
*/
public class SqlPractice {

	public static void main(String[] args) throws Exception {
		StringBuilder sql = new StringBuilder();
		sql.append("  SELECT  ");
		sql.append(" 	CASE WHEN (parentOrder.order_id IS NULL OR parentOrder.order_id = 0) THEN orderGoods.order_id ELSE parentOrder.order_id END order_id, ");
		sql.append("    CASE WHEN (parentOrder.order_id IS NULL OR parentOrder.order_id = 0) THEN `order`.order_sn ELSE parentOrder.order_sn END order_sn, ");
		sql.append(" 	`order`.pay_time `timestamp`, ");
		sql.append(" 	`order`.uid, ");
		sql.append(" 	`order`.user_id, ");
		sql.append(" 	`order`.mobile phone, ");
		sql.append(" 	`order`.consignee username, ");
		sql.append(" 	orderAddress.province_name province, ");
		sql.append(" 	orderAddress.city_name city, ");
		sql.append(" 	`order`.supplier_id, ");
		sql.append(" 	`order`.supplier_name, ");
		sql.append(" 	`order`.bonus_id, ");
		sql.append(" 	`order`.order_status, ");
		sql.append(" 	`order`.pay_status, ");
		sql.append("    `order`.shipping_fee, ");
		sql.append(" 	orderGoods.chat_record_id, ");
		sql.append(" 	orderGoods.cat_id sub_cat_id, ");
		sql.append(" 	category.cat_name sub_cat_name, ");
		sql.append(" 	category.parent_id p_cat_id, ");
		sql.append(" 	parentCategory.cat_name p_cat_name, ");
		sql.append(" 	sourceParam.name_cn `source`, ");
		sql.append(" 	orderGoods.order_id As goods_order_id, ");
		sql.append(" 	orderGoods.brand_id, ");
		sql.append(" 	brand.brand_name, ");
		sql.append(" 	orderGoods.rec_id order_goods_id, ");
		sql.append(" 	orderGoods.goods_id, ");
		sql.append(" 	orderGoods.product_id, ");
		sql.append(" 	orderGoods.product_sn, ");
		sql.append(" 	orderGoods.goods_name, ");
		sql.append(" 	orderGoods.goods_attr goods_sku, ");
		sql.append(" 	orderGoods.goods_price, ");
		sql.append(" 	orderGoods.goods_benefit, ");
		sql.append(" 	orderGoods.goods_number, ");
		sql.append(" 	orderGoods.goods_own_type, ");
		sql.append(" 	orderRebates.`type` share_order_type, ");
		sql.append(" 	orderRebates.mmc_type, ");
		sql.append("    orderRebates2.rebatesInfo, ");
		sql.append(" 	laxinInfo.lp_laxin_promotion_dim_id, ");
		sql.append(" 	laxinInfo.lp_laxin_promotion_range_vals, ");
		sql.append(" 	userDis.create_time vip_join_time, ");
		sql.append("    fmOrderTrack.`value` fm_code, ");
		sql.append("    outSideOrderTrack.`value` outside_code, ");
		sql.append("    inSideOrderTrack.`value` inside_code, ");
		sql.append("    promotionCoupon.promotion_name coupon_title, ");
		sql.append("    orderPromotion.promotion_type, ");
		sql.append("    promotionInfo.title promotion_title, ");
		sql.append("    msInfo.ms_title, ");
		sql.append("    supplierInfo.supplier_pay_method, ");
		sql.append("    supplierInfo.supplier_rate, ");
		sql.append(" 	subGoodsInfo.sub_order_goods_info, ");
		sql.append(" 	bbInfo.bb_births, ");
		sql.append("    reducePrice.reduce_price, ");
		sql.append("    goodsNumber.numbers, ");
		sql.append(" 	skuInfo.sub_sku_prices, ");
		sql.append("    jdOrder.order_price jdprice, ");
		sql.append("    jdOrder.freight     jdfreight, ");
		sql.append("    purchasePrice.purchase_price,  ");
		sql.append("    orderInfo.paytime_array,  ");
		sql.append("    orderInfo.last_paytime,  ");
		sql.append("    akcOrder.order_price     akcprice ");
		
		sql.append("	FROM bi_mall_order_goods orderGoods ");

		// 订单信息
		sql.append(" 	LEFT OUTER JOIN bi_mall_order_info `order` ON `order`.order_id = orderGoods.order_id ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_order_info parentOrder ON `order`.parent_order = parentOrder.order_id  ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_groupon_order groupon_order ON groupon_order.order_id = orderGoods.order_id ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_category category ON category.cat_id = orderGoods.cat_id  ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_category parentCategory ON parentCategory.cat_id = category.parent_id ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_source_param sourceParam ON sourceParam.`name` = `order`.`source` AND sourceParam.app_name = 'xsx'  ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_brand brand ON brand.brand_id = orderGoods.brand_id  ");
		sql.append(" 	LEFT OUTER JOIN ( ");
		sql.append("    	SELECT order_id, order_goods_id, concat_ws(',',collect_set(cast(type as string))) as type , concat_ws(',',collect_set(cast(mmc_type as string))) as mmc_type ");
		sql.append("		FROM bi_mall_order_rebates GROUP BY order_id,order_goods_id ");
		sql.append("    ) orderRebates ON orderRebates.order_id = orderGoods.order_id AND orderGoods.rec_id = orderRebates.order_goods_id  ");
		
		sql.append("	LEFT OUTER JOIN bi_mall_order_address orderAddress ON `order`.order_id = orderAddress.order_id ");

		// 拉新相关，使用 receive_limit = -3 (即未购买过妈妈良品的用户) 的优惠券
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" SELECT ");
		sql.append(" 	orderGoods.order_id order_id, ");
		sql.append(" 	orderGoods.product_id product_id, ");
		sql.append(" 	last(lpLaXinPromotionDim.id) lp_laxin_promotion_dim_id, ");
		sql.append(" 	concat_ws(',', collect_set(cast(lpLaXinPromotionRange.val as STRING))) lp_laxin_promotion_range_vals  ");
		sql.append(" FROM bi_mall_order_goods orderGoods ");
		sql.append(" LEFT OUTER JOIN bi_mall_order_info `order` ON `order`.order_id = orderGoods.order_id ");
		sql.append(" LEFT OUTER JOIN bi_mall_groupon_order groupon_order ON groupon_order.order_id = orderGoods.order_id ");
		sql.append(" LEFT OUTER JOIN bi_mall_promotion_code promotionCode ON `order`.bonus_id != '' AND  promotionCode.promotion_code = `order`.bonus_id ");
		sql.append(" LEFT OUTER JOIN bi_mall_promotion promotion ON promotion.id = promotionCode.promotion_id AND promotion.receive_limit = -3 ");
		sql.append(" LEFT OUTER JOIN bi_mall_promotion_range lpLaXinPromotionRange ON lpLaXinPromotionRange.type IN (1,3) AND lpLaXinPromotionRange.promotion_id = promotion.id ");
		sql.append(" LEFT OUTER JOIN bi_mall_order_promotion lpLaXinOrderPromotion ON lpLaXinOrderPromotion.order_id = orderGoods.order_id AND orderGoods.product_id = lpLaXinOrderPromotion.product_id AND lpLaXinOrderPromotion.promotion_type = 5 ");
		sql.append(" LEFT OUTER JOIN bi_mall_promotion_dimension lpLaXinPromotionDim ON lpLaXinOrderPromotion.promotion_id = lpLaXinPromotionDim.promotion_id AND lpLaXinPromotionDim.id = lpLaXinOrderPromotion.promotion_dimension_id AND lpLaXinPromotionDim.scope = 3 AND lpLaXinPromotionDim.`type` = 6 ");
		sql.append(" WHERE `order`.pay_time >= ${S_TIME} AND `order`.pay_time < ${E_TIME}  ");
		sql.append(" 	AND `order`.source_type != 2 AND `order`.app_name = 'xsx' AND orderGoods.goods_name NOT LIKE '%测试%' ");
		sql.append(" 	AND (groupon_order.`status`=3 OR groupon_order.`status` IS NULL) ");
		sql.append(" GROUP BY orderGoods.order_id, orderGoods.product_id ");
		sql.append(") laxinInfo ON laxinInfo.order_id = orderGoods.order_id AND laxinInfo.product_id=orderGoods.product_id ");
		//获取订单的组合商品数
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("       SELECT orderGoods.order_id, COUNT(orderGoods.order_id) AS numbers FROM bi_mall_order_goods orderGoods ");
		sql.append(" 	   LEFT OUTER JOIN bi_mall_order_info `order` ON `order`.order_id = orderGoods.order_id ");
		sql.append("       LEFT OUTER JOIN bi_mall_order_goods_link subOrderGoods ON subOrderGoods.order_id=orderGoods.order_id AND subOrderGoods.goods_id=orderGoods.goods_id ");
		sql.append(" 	   WHERE `order`.pay_time >= ${S_TIME} AND `order`.pay_time < ${E_TIME}  ");
		sql.append(" 	   AND `order`.source_type != 2 AND `order`.app_name = 'xsx' AND orderGoods.goods_name NOT LIKE '%测试%' ");
		sql.append("	   GROUP BY orderGoods.order_id ");
		sql.append(" ) goodsNumber ON goodsNumber.order_id = `order`.order_id ");
		//获取订单的运费券
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("	  SELECT   order_id, sum(reduce_price) as reduce_price ");
		sql.append("	  FROM  bi_mall_order_promotion WHERE promotion_type = 14 ");
		sql.append("	  GROUP BY order_id");
		sql.append(" ) reducePrice ON reducePrice.order_id = orderGoods.order_id ");		
		// 优惠相关
		sql.append("    LEFT OUTER JOIN bi_mall_promotion_code promotionCode ON `order`.bonus_id != '' AND  promotionCode.promotion_code = `order`.bonus_id ");
		// 优惠券
		sql.append(" 	LEFT OUTER JOIN bi_mall_promotion promotionCoupon ON promotionCoupon.id = promotionCode.promotion_id ");
		// 优惠活动
		sql.append("    LEFT OUTER JOIN ( SELECT order_id ,product_id ,concat_ws(',',collect_set(cast(promotion_type as STRING))) as promotion_type FROM bi_mall_order_promotion GROUP BY order_id ,product_id ) orderPromotion ON orderPromotion.order_id = orderGoods.order_id AND orderGoods.product_id = orderPromotion.product_id  ");
		sql.append("    LEFT OUTER JOIN ( ");
		sql.append("    	SELECT order_id,product_id,concat_ws('|',collect_set(title)) as title  FROM ( ");
		sql.append("    	SELECT orderPromotion.order_id,orderPromotion.product_id,promotionInfo.title FROM bi_mall_order_promotion  orderPromotion ");
		sql.append("    	LEFT OUTER JOIN bi_mall_promotion_info promotionInfo ON promotionInfo.id=orderPromotion.promotion_id  ) promotionInfo0 ");
		sql.append("    	GROUP BY order_id,product_id ) promotionInfo ON orderPromotion.order_id = promotionInfo.order_id AND promotionInfo.product_id = orderPromotion.product_id  ");
		sql.append("    LEFT OUTER JOIN ( ");
		sql.append("    	SELECT order_id,product_id,concat_ws('|',collect_set(ms_title)) as ms_title  FROM ( ");
		sql.append("    	SELECT orderPromotion.order_id,orderPromotion.product_id,msInfo.ms_title FROM bi_mall_order_promotion  orderPromotion ");
		sql.append("    	LEFT OUTER JOIN bi_mall_ms_info msInfo  ON msInfo.ms_id=orderPromotion.promotion_id  ) msInfo0 ");
		sql.append("    	GROUP BY order_id,product_id ) msInfo ON orderPromotion.order_id = msInfo.order_id AND msInfo.product_id = orderPromotion.product_id  ");
		// 良粉信息
		sql.append("    LEFT OUTER JOIN ( ");
		sql.append(" 		SELECT	user_id , status , min(create_time) create_time FROM bi_mall_user_distribution where status=1 GROUP BY user_id, status ");
		sql.append("	) userDis ON `order`.user_id=userDis.user_id AND userDis.status=1 ");

		// 渠道信息
		sql.append("    LEFT OUTER JOIN bi_mall_order_track fmOrderTrack ON fmOrderTrack.order_sn = `order`.order_sn AND fmOrderTrack.param_id = 1 AND fmOrderTrack.`value` REGEXP '^[0-9]+$'  ");
		sql.append("    LEFT OUTER JOIN bi_mall_order_track outSideOrderTrack ON outSideOrderTrack.order_sn = `order`.order_sn AND outSideOrderTrack.param_id = 3 AND outSideOrderTrack.`value` REGEXP '^[0-9]+$'  ");
		sql.append("    LEFT OUTER JOIN bi_mall_order_track inSideOrderTrack ON inSideOrderTrack.order_sn = `order`.order_sn AND inSideOrderTrack.param_id = 2 AND inSideOrderTrack.`value` REGEXP '^[0-9]+$'  ");

		// 组合商品信息及成本
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" SELECT ");
		sql.append(" 	orderGoods.order_id order_id, ");
		sql.append(" 	orderGoods.product_id product_id, ");
		sql.append("    concat_ws('Ξ', collect_set(concat_ws('Э',   ");
		sql.append(" 		CASE WHEN subOrderGoods.link_erp_goods_id IS NULL THEN '0' ELSE subOrderGoods.link_erp_goods_id END,  ");//0
		sql.append("        CASE WHEN subOrderGoods.batch_num IS NULL THEN '无记录' ELSE subOrderGoods.batch_num END, ");//1
		sql.append("        CASE WHEN subOrderGoods.link_product_id IS NULL THEN '0' ELSE subOrderGoods.link_product_id END, ");//2
		sql.append("        CASE WHEN subOrderGoods.link_product_sn IS NULL THEN '0' ELSE subOrderGoods.link_product_sn END, ");//3
		sql.append("        CASE WHEN subOrderGoods.link_goods_id IS NULL THEN '0' ELSE subOrderGoods.link_goods_id END, ");//4
		sql.append("        CASE WHEN subOrderGoods.link_goods_name IS NULL THEN '无记录' ELSE subOrderGoods.link_goods_name END, ");//5
		sql.append("        CASE WHEN subOrderGoods.promote_price IS NULL THEN '0' ELSE subOrderGoods.promote_price END, ");//6
		sql.append("        CASE WHEN subOrderGoods.promote_number IS NULL THEN '0' ELSE subOrderGoods.promote_number END, ");//7
		sql.append("        CASE WHEN subOrderGoodsAttr.attr_value IS NULL THEN '无记录' ELSE subOrderGoodsAttr.attr_value END, ");//8
		sql.append("        skuInfo.sub_sku_prices))) sub_order_goods_info ");//9
		sql.append(" 	FROM bi_mall_order_goods orderGoods ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_order_info `order` ON `order`.order_id = orderGoods.order_id ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_order_goods_link subOrderGoods ON subOrderGoods.order_id=orderGoods.order_id AND subOrderGoods.goods_id=orderGoods.goods_id ");
		sql.append(" 	LEFT OUTER JOIN bi_mall_goods_attr subOrderGoodsAttr ON subOrderGoodsAttr.goods_attr_id=subOrderGoods.goods_attr ");
		
		sql.append(" 	LEFT OUTER JOIN ( ");
		sql.append(" 		SELECT ");
		sql.append("    		skuPrice.barcode barcode,   ");
		sql.append("    		concat_ws('@', collect_set(concat_ws('#',   ");
		sql.append(" 				skuPrice.create_time,  ");
		sql.append(" 				skuPrice.price  ");
		sql.append("        	))) sub_sku_prices ");
		sql.append(" 		FROM  bi_erp_sku_price skuPrice ");
		sql.append(" 		GROUP BY skuPrice.barcode ");
		sql.append(" 		) skuInfo ON skuInfo.barcode=subOrderGoods.link_product_sn ");
		
		sql.append(" 	WHERE `order`.pay_time >= ${S_TIME} AND `order`.pay_time < ${E_TIME}  ");
		sql.append(" 		AND `order`.source_type != 2 AND `order`.app_name = 'xsx' AND orderGoods.goods_name NOT LIKE '%测试%' ");
		sql.append(" 		AND orderGoods.goods_own_type = 2 ");//组合商品
		sql.append(" 	GROUP BY orderGoods.order_id, orderGoods.product_id ");
		sql.append(" ) subGoodsInfo ON subGoodsInfo.order_id = orderGoods.order_id AND subGoodsInfo.product_id=orderGoods.product_id ");

		// 订单非组合商品成本
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" 	SELECT ");
		sql.append("    	skuPrice.barcode barcode,   ");
		sql.append("    	concat_ws('@', collect_set(concat_ws('#',   ");
		sql.append(" 			skuPrice.create_time,  ");
		sql.append(" 			skuPrice.price  ");
		sql.append("        ))) sub_sku_prices ");
		sql.append(" 	FROM  bi_erp_sku_price skuPrice ");
		sql.append(" 	GROUP BY skuPrice.barcode ");
		sql.append(" ) skuInfo ON skuInfo.barcode=orderGoods.product_sn ");
		
		// 京东订单的有效订单成本
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("   SELECT  xsx_order_sn , sum(order_price) as order_price , sum(freight) as freight ");
		sql.append("   FROM    bi_erp_jd_order WHERE order_state = 1 GROUP BY xsx_order_sn ");
		sql.append(" ) jdOrder ON jdOrder.xsx_order_sn = `order`.order_sn AND `order`.pay_status = 2 ");
		
		// 爱库存订单成本
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("   SELECT  xsx_order_sn , sum(total_amount) as order_price ");
		sql.append("   FROM    bi_erp_akc_order where payment_status = '3' GROUP BY xsx_order_sn ");
		sql.append(" ) akcOrder ON akcOrder.xsx_order_sn = `order`.order_sn AND `order`.pay_status = 2 ");
		
		//返现数据
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("    	SELECT order_id, order_goods_id, ");
		sql.append("    	concat_ws('@', collect_set(concat_ws('#',   ");
		sql.append(" 			type,           ");//返现类型
		sql.append("            status,         ");//结算状态
		sql.append("            mmc_type,       ");//良粉会员
		sql.append("            user_level,     ");//分销会员等级
		sql.append(" 			rebates_amount  ");//返现金额
		sql.append("        ))) rebatesInfo ");
		sql.append("		FROM bi_mall_order_rebates GROUP BY order_id,order_goods_id ");
		sql.append(" ) orderRebates2 ON orderRebates2.order_id = orderGoods.order_id AND orderGoods.rec_id = orderRebates2.order_goods_id  ");
		
		// 供应商
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" SELECT ");
		sql.append(" 	`order`.order_id order_id, ");
		sql.append(" 	supplier.id supplier_id, ");
		sql.append(" 	last(supplier.payMethodId) supplier_pay_method, ");
		sql.append(" 	last(supplier_comm.rate) supplier_rate ");
		sql.append(" FROM bi_mall_order_info `order` ");
		sql.append(" LEFT OUTER JOIN bi_mall_groupon_order groupon_order ON groupon_order.order_id = `order`.order_id ");
		sql.append(" LEFT OUTER JOIN bi_suppuser supplier ON supplier.id = `order`.supplier_id  ");
		sql.append(" LEFT OUTER JOIN bi_suppcommission supplier_comm ON `order`.supplier_id = supplier_comm.userId AND unix_timestamp(concat_ws('', supplier_comm.beginTime)) <= `order`.pay_time AND unix_timestamp(concat_ws('', supplier_comm.endTime)) > `order`.pay_time ");
		sql.append(" WHERE `order`.pay_time >= ${S_TIME} AND `order`.pay_time < ${E_TIME}  ");
		sql.append(" 	AND `order`.source_type != 2 AND `order`.app_name = 'xsx' ");
		sql.append(" 	AND (groupon_order.`status`=3 OR groupon_order.`status` IS NULL) ");
		sql.append(" GROUP BY `order`.order_id, supplier.id ");
		sql.append(" ) supplierInfo ON supplierInfo.supplier_id=`order`.supplier_id AND supplierInfo.order_id=`order`.order_id ");
		//部分供应商成本
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append("  SELECT ");
		sql.append(" 	 supplier_id, ");
		sql.append(" 	 product_sn, ");
		sql.append("     concat_ws('@', collect_set(concat_ws('#',   ");
		sql.append(" 			    add_time,  ");
		sql.append(" 				purchase_price	  ");
		sql.append("      ))) purchase_price ");
		sql.append("  FROM bi_mall_purchase_price ");
		sql.append("  GROUP BY supplier_id , product_sn ");
		sql.append(" ) purchasePrice ON purchasePrice.supplier_id = `order`.supplier_id AND purchasePrice.product_sn = orderGoods.product_sn ");
		// 用户信息
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" SELECT ");
		sql.append(" 	bb_info.uid, ");
		sql.append(" 	concat_ws(',', collect_set(bb_info.birthday)) bb_births ");
		sql.append(" FROM uc_members_baby_info bb_info ");
		sql.append(" GROUP BY bb_info.uid ");
		sql.append(" ) bbInfo ON bbInfo.uid=`order`.uid ");
		
		//获取订单顺序
		//获取最近订单的支付时间,用于获取用户活跃度
		sql.append(" LEFT OUTER JOIN ( ");
		sql.append(" SELECT ");
		sql.append(" 	order_info.uid, ");
		sql.append(" 	last(order_info.pay_time) last_paytime, ");
		sql.append(" 	concat_ws(',', collect_set(cast(order_info.pay_time as STRING))) paytime_array ");
		sql.append(" FROM bi_mall_order_info order_info ");
		sql.append(" 	WHERE ");
		sql.append(" 	order_info.pay_time >= ${S_TIME} AND order_info.pay_time < ${E_TIME} ");
		sql.append(" 	AND order_info.source_type != 2  ");
		sql.append(" 	AND order_info.app_name = 'xsx' ");
		sql.append(" GROUP BY order_info.uid ");
		sql.append(" ) orderInfo ON orderInfo.uid=`order`.uid ");

		sql.append(" 	WHERE ");
		sql.append(" 	`order`.pay_time >= ${S_TIME} AND `order`.pay_time < ${E_TIME} ");
		sql.append(" 	AND `order`.source_type != 2  ");
		sql.append(" 	AND `order`.app_name = 'xsx' ");
		sql.append(" 	AND orderGoods.goods_name NOT LIKE '%测试%'  ");

		// 去除定金团的订单
		sql.append(" 	AND (groupon_order.`status`=3 OR groupon_order.`status` IS NULL)  ");
		
		System.out.println(sql.toString());
		
	}
}
