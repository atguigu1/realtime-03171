import com.atguigu.realtime.bean.EventLog
import org.json4s.jackson.JsonMethods

/**
 * Author atguigu
 * Date 2020/8/21 9:18
 */
object Json4sDemo {
    def main(args: Array[String]): Unit = {
        
        val s =
            """
              |{"logType":"event","area":"guangdong","uid":"uid1140","eventId":"addCart","itemId":26,"os":"android","nextPageId":44,"appId":"gmall","mid":"mid_37","pageId":38}
              |""".stripMargin
        
        import org.json4s.JsonDSL._
        /*implicit  val f = org.json4s.DefaultFormats
        val j = JsonMethods.parse(s).extract[User]*/
        implicit val f = org.json4s.DefaultFormats
    }
}

case class User(name: String, age: Int)
