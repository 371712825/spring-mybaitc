package XiaoTest.practice;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("bc")
public class BbController {

	@RequestMapping("bc")
	public String bc() {
		return "bc";
	}
}
