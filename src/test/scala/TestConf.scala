import com.sgr.util.{Conf, LazyLogging}
import org.scalatest.funsuite.AnyFunSuite
/**
 * @author sunguorui
 * @date 2021年10月19日 10:11 上午
 */
class TestConf extends AnyFunSuite with LazyLogging{

  test("测试conf") {
    assertResult("/car_data") {
      Conf.rootPath
    }
  }

}
