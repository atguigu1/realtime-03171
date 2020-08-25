package com.atguigu.gmallpublisher;

/**
 * @Author lzc
 * @Date 2020/8/25 15:08
 */
public class ExpDemo {
    public static void main(String[] args) {
        /*String s = "a";
        boolean r = s.matches("[a-c]?");
        System.out.println(r);*/
        /*String tel = "abc@sohu.com.cn";
        // 判断电话号码是否正确
//        boolean r = tel.matches("1[3-9]\\d{9}");
        boolean r = tel.matches("\\w{3,15}@\\S+\\.(com|cn|org|edu|com\\.cn)");
        System.out.println(r);*/


        /*String s = "abc2caba3aea4f";
        String r = s.replaceAll("([a-z])\\d\\1", "$1$1");
        System.out.println(r);*/
        /*String s = "我我我今今今天晚上要要请你们们洗脚";
        System.out.println(s.replaceAll("(.)\\1+", "$1"));
        */

        /*String ip = "afafafa192aaafa168fasdlfja1fa  sdlfjlasd???  jkf10";
        String[] split = ip.split("\\D+");

        System.out.println(Arrays.toString(split));*/
        /*String s = "aadfjalfjlasdjflasdbc1a2ba3aslfjlasdjeljfalksdjflkasdjflksdajflasdfjasa";
        System.out.println(s.split("\\D").length);*/

    }
}
/*
[abc]   表示a或者b或者c
[a-z]   匹配任意小写字母
[a-de-h]
[a-zA-Z0-9_]  数字字母下划线

[^abc]  非 a 非b 非c

\w  w:word 单词字符  === [a-zA-Z0-9_]
\W  非单词字符
\d  d:digital 数字  [0-9]
\D  非数字
\s   s:space 空白字符串  空格 \r \n \t
\S  非空白字符串

.   任意字符串    \n \r
\.  表示一个点

针对量词:
    贪婪模式
    懒惰模式

  ^a 不在方括号内: 表示a开头
  b$  b结尾


----
提高:
数量词
 a{n} 正好n个a
 a{n,} 至少n个
 a{n,m} 至少n个最多m个  >=n  <=m
 a+  至少1个  等价于a{1,}
 a*  至少0个
 a?  0个或1个




正则表达式:
什么是正则?
    是一个工具, 用来处理文本的强大工具!

    作用:
     1. 用来判断字符串是否满足要求
     2. 用来从一大段文本获取想要的那些字符串(爬虫)

     正则一段有特殊意义的字符串

java中如何使用?

    1. 最基本的使用
            Pattern
            Matcher
    2. java的字符串类型, 提供了四个方法, 直接支持正则
        一帮这个4个方法就足够了

        matches
        replaceAll
        replaceFirst   ?
        split

 */