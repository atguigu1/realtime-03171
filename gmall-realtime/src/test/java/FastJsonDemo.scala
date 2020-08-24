import com.alibaba.fastjson.JSON
import org.json4s.jackson.Serialization

import scala.beans.BeanProperty

/**
 * Author atguigu
 * Date 2020/8/24 14:14
 */
object FastJsonDemo {
    def main(args: Array[String]): Unit = {
        
        
        val a = A(10, "aaa")
        /*println(JSON.toJSONString(a, true))*/
        implicit val f = org.json4s.DefaultFormats
        println(Serialization.write(a))
        
    }
}

case class A( a: Int,  b: String)
