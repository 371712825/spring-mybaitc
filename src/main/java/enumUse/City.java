package enumUse;

public enum City {

	BJ("北京","北京市"),SH("上海","上海市"),TJ("天津","天津市"),CQ("重庆","重庆市"),HLJ("黑龙江省","哈尔滨"),JLCC("吉林省","长春"),LNSY("辽宁省","沈阳"),HBSJZ("河北省","石家庄"),HNZZ("河南省","郑州"),SDJN("山东省","济南"),JSNJ("江苏省","南京"),AHHF("安徽省","合肥"),ZJHZ("浙江省","杭州"),HBWH("湖北省","武汉"),FJFZ("福建省","福州"),JXNC("江西省","南昌"),HNCS("湖南省","长沙"),GDGZ("广东省","广州"),GXNN("广西省","南宁"),TWTV("台湾省","台北"),GZGY("贵州省","贵阳"),SCCD("四川省","成都"),SXXA("陕西省","西安"),SXTY("山西省","太原"),GSLZ("甘肃省","兰州"),YNKM("云南省","昆明"),NXYC("宁夏省","银川"),XJWLMQ("新疆省","乌鲁木齐"),XZLS("西藏省","拉萨"),QHXN("青海省","西宁"),NMG("内蒙古省","呼和浩特"),HNHK("海南省","海口"),XG("香港省","香港"),AM("澳门省","澳门");
	
	private String province;
	
	private String city;
	
	private City(String province, String city){
		this.province = province;
		this.city = city;
	}
	
	public String getProvince() {
		return this.province;
	}
	
	public String getCity() {
		return this.city;
	}
	
	public static String getProvinceByCity(String city) {
		String result = null;
		if (!city.isEmpty()) {
			for (City c : City.values()) {
				if (city.equals(c.getCity())) {
					result = c.getProvince();
					break;
				}
			}
		}
		return result;
	}
	
	public static String getCityByProvince(String province) {
		String result = null;
		if (!province.isEmpty()) {
			for (City c : City.values()) {
				if (province.equals(c.getProvince())) {
					result = c.getCity();
					break;
				}
			}
		}
		return result;
	}
	
}
