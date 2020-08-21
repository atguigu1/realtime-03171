

/**
 * Author atguigu
 * Date 2020/8/18 10:29
 */
object Test {
    def main(args: Array[String]): Unit = {
        
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        
        try {
            arr1.foreach(x => {
                if (x > 50) return
                println(x)
            })
        }catch {
            case e =>
                println(e)
        }
        
        System.out.println("abc.....")
        
    }
    
    def foo(x: Int): Unit = {
        
        // 后面....
    }
}
