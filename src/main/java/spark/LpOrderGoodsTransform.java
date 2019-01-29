package spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


public class LpOrderGoodsTransform {
	private static Logger logger = LoggerFactory.getLogger(LpOrderGoodsTransform.class);

	public void transform(SparkSession spark, String... args) throws Exception{
		Long stime = null;
		Long etime = null;
		if (args != null && args.length == 2) {
			stime = new DateTime(args[0]).getMillis() / 1000l;
			etime = new DateTime(args[1]).getMillis() / 1000l;
		} else {
			stime = new DateTime("2016-01-01").getMillis() / 1000l;
			etime = new DateTime().withMillisOfDay(0).getMillis() / 1000l;
		}
		
		FSDataOutputStream output = HdfsUtils.getInstance().create(new Path("/data/bispark/lpTf-" + String.valueOf(new DateTime(stime * 1000l).toString("yyyyMMdd") + "-" + String.valueOf(new DateTime(etime * 1000l).toString("yyyyMMdd") + "/data.json"))), true);
		Dataset<Row> data = spark.read().json(HdfsUtils.HOST + "/data/bispark/lpEx-" + String.valueOf(new DateTime(stime * 1000l).toString("yyyyMMdd") + "-" + String.valueOf(new DateTime(etime * 1000l).toString("yyyyMMdd"))) + "/*.json");
		Iterator<String> it = data.toJSON().toLocalIterator();
		
		//转换逻辑
		JSONObject extractRecord = null;
		JSONObject newRecord = null;
		List<String> bbBirthsList = null;
		byte[] newLineBytes = "\n".getBytes("UTF-8");
		byte[] tempBytes = null;
		
		while(it.hasNext()){
			extractRecord = JSONObject.parseObject(it.next());
			newRecord = new JSONObject();
			
			//设置基本信息
			this.setBasicInfo(newRecord, extractRecord);
			
			//过滤出有效的 宝宝生日
			if(StringUtils.isNotBlank(extractRecord.getString("bb_births")) 
					&& (bbBirthsList = Arrays.asList(extractRecord.getString("bb_births").split(","))) != null
					&& bbBirthsList.size() > 0){
				ListIterator<String> tmpIt = bbBirthsList.listIterator();
				while(tmpIt.hasNext()){
					String eL = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
					Pattern p = Pattern.compile(eL);
					Matcher m = p.matcher(tmpIt.next());
					if (!m.matches()){
						tmpIt.remove();
					}
				}
			}
			
			//计算宝宝生日, 按最新的算 TODO (往后MYSQL这里要支持数组查询(列转行))
			if(bbBirthsList != null && bbBirthsList.size() > 0){
				//newRecord.put("bb_days", BBStatusCalUtils.getBbDays(new DateTime(bbBirthsList.get(bbBirthsList.size() - 1)), new DateTime(extractRecord.getLongValue("timestamp") * 1000l)));
				//newRecord.put("bb_status", BBStatusCalUtils.getBBStatusByDaysSimpleVer(newRecord.getLongValue("bb_days")));
			}
			
			if(StringUtils.isNotBlank(extractRecord.getString("share_order_type"))){
				String shareOrderType = extractRecord.getString("share_order_type");
				//一个订单可能存在多种类型
				List<String> sotList = Arrays.asList(shareOrderType.split(","));
				
				for (String sotTemp : sotList) {
					switch(sotTemp) {
						case "1" : newRecord.put("share_order_type", "邀请单");break;
						case "2" : newRecord.put("share_order_type", "分享单");break;
						case "3" : newRecord.put("share_order_type", "自购单");break;
						default : ;
					}
				}
				
			}
			
			// 0:促销方式是优惠活动, 非0:促销方式是优惠券, 一个订单只会存在一种促销方式
			
			if(StringUtils.isNotBlank(extractRecord.getString("promotion_type"))){
				String[] promotion_types=extractRecord.getString("promotion_type").split(",");
				if(StringUtils.isBlank(newRecord.getString("promotion_type"))||"无促销".equals(newRecord.getString("promotion_type")))
					newRecord.put("promotion_type","");
				if(StringUtils.isNotBlank(extractRecord.getString("promotion_title"))){
					newRecord.put("promotion_title", extractRecord.getString("promotion_title"));
				}
				boolean exist3=false,exist9=false;
				for(String type:promotion_types){
					String prefix=newRecord.getString("promotion_type");
					if(StringUtils.isNotBlank(prefix))
						prefix+="|";
					switch(type) {
						case "1" : newRecord.put("promotion_type", prefix+"定金团"); break;
						case "2" : newRecord.put("promotion_type", prefix+"拼货团"); break;
						case "3" : exist3=true;newRecord.put("promotion_type", prefix+"秒杀");break;
						case "5" : newRecord.put("promotion_type", prefix+"专享价"); break;
						case "6" : newRecord.put("promotion_type", prefix+"满赠"); break;
						case "7" : newRecord.put("promotion_type", prefix+"满减"); break;
						case "9" : exist9=true;newRecord.put("promotion_type", prefix+"优惠券"); break;						
						case "10" : newRecord.put("promotion_type", prefix+"预售"); break;
						case "14" : newRecord.put("promotion_type", prefix+"运费券"); break;
						default :   newRecord.put("promotion_type", prefix+"未知优惠类型");
					}
				}
				if(exist9 && StringUtils.isNotBlank(extractRecord.getString("coupon_title"))){
					//存在优惠券并且优惠券名称不为空
					if("无促销".equals(newRecord.getString("promotion_title")))
						newRecord.put("promotion_title", extractRecord.getString("coupon_title"));
					else
						newRecord.put("promotion_title", newRecord.getString("promotion_title")+"|"+extractRecord.getString("coupon_title"));
				}
				if(exist3 && StringUtils.isNotBlank(extractRecord.getString("ms_title"))){
					//存在秒杀且秒杀活动名称不为空
					if("无促销".equals(newRecord.getString("promotion_title")))
						newRecord.put("promotion_title", extractRecord.getString("ms_title"));
					else
						newRecord.put("promotion_title", newRecord.getString("promotion_title")+"|"+extractRecord.getString("ms_title"));
				}
				
				//校验|的空处理以及去除含有测试字段的数据
				if (newRecord.containsKey("promotion_title") && StringUtils.isNotBlank(newRecord.getString("promotion_title"))) {
					String ptTemp = newRecord.getString("promotion_title");
					
					if (ptTemp.contains("|")) { 
						String[] newRecordTemps = ptTemp.split("\\|");
						List<String> tempList = new LinkedList<>();
						
						for (String str : newRecordTemps) {
							if (StringUtils.isNotBlank(str) && !str.contains("测试")) {
								tempList.add(str);
							}
						}
						
						if (tempList.size() > 0) {
							newRecord.put("promotion_title", StringUtils.join(tempList.toArray(), "|"));
						}
						
					} else {
						if (ptTemp.contains("测试")) {
							newRecord.put("promotion_title", "未录入");
						}
					}
				}
				
			}
			

			List<String> lpLaxinPromotionRangeVals = null;
			if(StringUtils.isNotBlank(extractRecord.getString("lp_laxin_promotion_range_vals"))
					&& ((lpLaxinPromotionRangeVals = Arrays.asList(extractRecord.getString("lp_laxin_promotion_range_vals").split(","))).contains("1146") //如果包含1146说明是良品品牌专用优惠券
							|| lpLaxinPromotionRangeVals.contains(extractRecord.getString("goods_id"))//优惠范围包含当前的商品
							|| StringUtils.isNotBlank(extractRecord.getString("lp_laxin_promotion_dim_id")))) {//能找到活动ID的
				newRecord.put("is_laxin_order_goods", "拉新订单商品");
			} else {
				newRecord.put("is_laxin_order_goods", "普通订单商品");
			}
			
			//子商品逻辑
			if(StringUtils.isNotBlank(extractRecord.getString("goods_own_type")) && "2".equals(extractRecord.getString("goods_own_type"))
					&& StringUtils.isNotBlank(extractRecord.getString("sub_order_goods_info"))){
				String[] subOrderGoodsInfoArr = extractRecord.getString("sub_order_goods_info").split("Ξ");

				//组合商品下的全部子商品销售金额之和
				Double saleAmountSub = 0d;
				for(int i=0 ; i<subOrderGoodsInfoArr.length ; i++){
					String[] subOrderGoodsInfo = subOrderGoodsInfoArr[i].split("Э");
					saleAmountSub += (extractRecord.getDoubleValue("goods_number") * Double.valueOf(subOrderGoodsInfo[6]) * Double.valueOf(subOrderGoodsInfo[7]));
				}
				
				//非扣点结算的子商品成本之和
				for(int i=0 ; i<subOrderGoodsInfoArr.length ; i++){
					String[] subOrderGoodsInfo = subOrderGoodsInfoArr[i].split("Э");
					
					if(subOrderGoodsInfo.length != 10) {
						logger.error("子商品切割长度异常:" + extractRecord.toJSONString());
						continue;
					}
					
					if("0".equals(subOrderGoodsInfo[2])){ // link_product_id 为 0
						logger.error("子商品信息有误: " + extractRecord.toJSONString());
						continue;
					}
					
					// 组合商品的 goods_number * goods_price != SUM(子商品的 promotion_price * promotion_number * goods_number), 需要计算分摊比例
					Double shareRate = (extractRecord.getDoubleValue("goods_number") * Double.valueOf(subOrderGoodsInfo[6]) * Double.valueOf(subOrderGoodsInfo[7])) / saleAmountSub;
					
					//重新设置基本信息
					newRecord.put("goods_id", subOrderGoodsInfo[4]);
					newRecord.put("product_id", subOrderGoodsInfo[2]);
					newRecord.put("product_sn", subOrderGoodsInfo[3]);
					newRecord.put("goods_name", subOrderGoodsInfo[5]);
					newRecord.put("goods_sku", subOrderGoodsInfo[8]);
					newRecord.put("sale_amount", shareRate * extractRecord.getDoubleValue("goods_number") * extractRecord.getDoubleValue("goods_price"));
					newRecord.put("goods_benefit", shareRate * extractRecord.getDoubleValue("goods_benefit"));
					newRecord.put("money_paid", newRecord.getDoubleValue("sale_amount") - newRecord.getDoubleValue("goods_benefit"));
					newRecord.put("goods_number", Integer.valueOf(subOrderGoodsInfo[7]) * extractRecord.getInteger("goods_number"));
					
					//成本计算
					if("2".equals(extractRecord.getString("supplier_pay_method"))){
						//扣点结算
						newRecord.put("first_cost", (extractRecord.getDoubleValue("goods_number") * extractRecord.getDoubleValue("goods_price") - extractRecord.getDoubleValue("goods_benefit")) * ((100d - extractRecord.getDoubleValue("supplier_rate")) * 0.01d));
					} else {
						newRecord.put("first_cost", this.getLatestSubSkuPrice(subOrderGoodsInfo[9], extractRecord.getLongValue("timestamp") * 1000l) * newRecord.getInteger("goods_number"));
					}
					
					tempBytes = newRecord.toJSONString().getBytes("UTF-8");
					output.write(tempBytes, 0, tempBytes.length);
					output.write(newLineBytes, 0, newLineBytes.length);
				}
				
			} else {
				//成本计算
				if("2".equals(extractRecord.getString("supplier_pay_method"))){
					//扣点结算
					newRecord.put("first_cost", newRecord.getDoubleValue("money_paid") * ((100d - extractRecord.getDoubleValue("supplier_rate")) * 0.01d));
				} else {
					newRecord.put("first_cost", this.getLatestSubSkuPrice(extractRecord.getString("sub_sku_prices"), extractRecord.getLongValue("timestamp") * 1000l) * extractRecord.getInteger("goods_number"));
					
					Integer goodsNumber=1;
					if(extractRecord.getIntValue("numbers")!=0)
						goodsNumber=extractRecord.getIntValue("numbers");
					//计算京东成本,京东订单应该没有扣点结算,所以在这里处理
					if("2496".equals(extractRecord.getString("supplier_id"))){
						//把订单价格平摊到每个商品
						newRecord.put("first_cost",(extractRecord.getDoubleValue("jdprice")+extractRecord.getDoubleValue("jdfreight"))/goodsNumber);
					}
					//爱库存成本,与京东类似
					if("2575".equals(extractRecord.getString("supplier_id"))){
						newRecord.put("first_cost",extractRecord.getDoubleValue("akcprice")/goodsNumber);
					}
					//部分供应商成本计算
					if( !"2496".equals(extractRecord.getString("supplier_id"))
							&& !"2575".equals(extractRecord.getString("supplier_id"))
							&& ("0.0".equals(newRecord.getString("first_cost")) || StringUtils.isBlank(newRecord.getString("first_cost"))) )
						newRecord.put("first_cost", this.getLatestSubSkuPrice(extractRecord.getString("purchase_price"), extractRecord.getLongValue("timestamp") * 1000l) * extractRecord.getInteger("goods_number"));
				}
				//各种转换各种计算
				tempBytes = newRecord.toJSONString().getBytes("UTF-8");
				output.write(tempBytes, 0, tempBytes.length);
				output.write(newLineBytes, 0, newLineBytes.length);
			}
		}
		
		output.flush();
		output.close();
	}
	
	private double getLatestSubSkuPrice(String subSkuPricesStr, long payTime){
		if(StringUtils.isBlank(subSkuPricesStr)){
			return 0d;
		}
		
		String[] subSkuPricesArr = subSkuPricesStr.split("@");
		
		double skuPirce = 0d;
		DateTime lastDate = null;
		DateTime currDate = null;
		DateTime payDate = new DateTime(payTime * 1000l);
		
		for(int i=0 ; i<subSkuPricesArr.length ; i++){
			String[] subSkuPrices = subSkuPricesArr[i].split("#");
			if((currDate = new DateTime(Long.valueOf(subSkuPrices[0]) * 1000l)).isAfter(payDate)){
				continue;
			}
			
			if(currDate.isAfter(lastDate) || lastDate == null){
				skuPirce = Double.valueOf(subSkuPrices[1]);
				lastDate = currDate;
			}
		}
		
		return skuPirce;
	}
	
	public void setBasicInfo(JSONObject newRecord, JSONObject extractRecord){
		newRecord.put("uid", extractRecord.getString("uid"));
		newRecord.put("user_id", extractRecord.getString("user_id"));
		newRecord.put("timestamp", extractRecord.getLongValue("timestamp"));
		newRecord.put("province", extractRecord.getString("province"));
		newRecord.put("city", extractRecord.getString("city"));
		newRecord.put("phone", extractRecord.getString("phone"));
		newRecord.put("supplier_id", extractRecord.getString("supplier_id"));
		newRecord.put("supplier_name", extractRecord.getString("supplier_name"));
		newRecord.put("order_id", extractRecord.getString("order_id"));
		newRecord.put("order_sn", extractRecord.getString("order_sn"));
		newRecord.put("brand_id", extractRecord.getString("brand_id"));
		
		//chat业务加入
		newRecord.put("chat_record_id", extractRecord.getString("chat_record_id"));
		newRecord.put("pay_status", extractRecord.getInteger("pay_status"));
		
		newRecord.put("brand_name", extractRecord.getString("brand_name"));
		newRecord.put("p_cat_id", extractRecord.getString("p_cat_id"));
		newRecord.put("p_cat_name", extractRecord.getString("p_cat_name"));
		newRecord.put("sub_cat_id", extractRecord.getString("sub_cat_id"));
		newRecord.put("sub_cat_name", extractRecord.getString("sub_cat_name"));
		newRecord.put("source", extractRecord.getString("source"));
		newRecord.put("fm", extractRecord.getString("fm_code"));
		newRecord.put("outside", extractRecord.getString("outside_code"));
		newRecord.put("inside", extractRecord.getString("inside_code"));
		newRecord.put("is_delete", 1);
		newRecord.put("vip_join_time", extractRecord.getString("vip_join_time"));
		newRecord.put("goods_id", extractRecord.getString("goods_id"));
		newRecord.put("product_id", extractRecord.getString("product_id"));
		newRecord.put("product_sn", extractRecord.getString("product_sn"));
		newRecord.put("goods_name", extractRecord.getString("goods_name"));
		newRecord.put("goods_sku", extractRecord.getString("goods_sku"));
		newRecord.put("sale_amount", extractRecord.getDoubleValue("goods_number") * extractRecord.getDoubleValue("goods_price"));
		newRecord.put("money_paid", newRecord.getDoubleValue("sale_amount") - extractRecord.getDoubleValue("goods_benefit"));
		newRecord.put("goods_benefit", extractRecord.getDoubleValue("goods_benefit"));
		newRecord.put("goods_number", extractRecord.getInteger("goods_number"));

		//把运费和运费券平均分摊到每个商品
		Integer goodsNumber=1;
		if(extractRecord.getIntValue("numbers")!=0)
			goodsNumber=extractRecord.getIntValue("numbers");
		newRecord.put("reduce_price", (double)(extractRecord.getDoubleValue("reduce_price")/goodsNumber) );//分摊运费券
		newRecord.put("shipping_fee", (double)(extractRecord.getDoubleValue("shipping_fee")/goodsNumber) );//分摊运费

		newRecord.put("share_order_type", "普通单");
		newRecord.put("promotion_title", "无促销");
		newRecord.put("promotion_type", "无促销");
		newRecord.put("mmc_type", extractRecord.getString("mmc_type"));
		
		if(StringUtils.isBlank(newRecord.getString("fm"))){
			newRecord.put("fm", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("outside"))){
			newRecord.put("outside", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("inside"))){
			newRecord.put("inside", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("vip_join_time"))){
			newRecord.put("vip_join_time", 0);
		}
		if(StringUtils.isBlank(newRecord.getString("p_cat_id"))){
			newRecord.put("p_cat_id", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("p_cat_name"))){
			newRecord.put("p_cat_name", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("sub_cat_id"))){
			newRecord.put("sub_cat_id", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("sub_cat_name"))){
			newRecord.put("sub_cat_name", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("brand_id"))){
			newRecord.put("brand_id", -1);
		}
		if(StringUtils.isBlank(newRecord.getString("brand_name"))){
			newRecord.put("brand_name", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("source"))){
			newRecord.put("source", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("province"))){
			newRecord.put("province", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("city"))){
			newRecord.put("city", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("phone"))){
			newRecord.put("phone", "未录入");
		}
		if(StringUtils.isBlank(newRecord.getString("mmc_type"))){
			newRecord.put("mmc_type", "未录入");
		}
	}
}
