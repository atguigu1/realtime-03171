package com.atguigu.realtime.bean


case class OrderInfo(id: String,
                     province_id: String,
                     var consignee: String,
                     order_comment: String,
                     var consignee_tel: String,
                     order_status: String,
                     payment_way: String,
                     user_id: String,
                     img_url: String,
                     total_amount: Double,
                     expire_time: String,
                     delivery_address: String,
                     create_time: String,
                     operate_time: String,
                     tracking_no: String,
                     parent_order_id: String,
                     out_trade_no: String,
                     trade_body: String,
                     var create_date: String = null,
                     var create_hour: String = null){
    
    // 2020-08-19 00:08:13   create_time
    create_date = create_time.substring(0, 10)
    create_hour = create_time.substring(11, 13)
    
    // 对用户的名字和电话号码做脱敏处理  186****1111
    consignee = consignee.substring(0, 1) + "**"
    consignee_tel = consignee_tel.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2")
}
