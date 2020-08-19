package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/8/19 15:28
 */
public interface OrderMapper {
    Double getTotalAmount(String date);

    /*
    +--------------+---------+
| CREATE_HOUR  |   SUM   |
+--------------+---------+
| 02           | 484     |
| 04           | 888     |
| 05           | 1068    |
| 09           | 5.4E+2  |
| 10           | 205     |
| 13           | 984     |
| 14           | 1055    |
| 16           | 1694    |
| 18           | 207     |
| 20           | 705     |
| 21           | 1162    |

     */
    List<Map<String, Object>> getHourAmount(String date);
}
