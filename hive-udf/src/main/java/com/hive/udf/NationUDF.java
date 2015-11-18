package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xianwei on 2015/8/21.
 * hive自定义函数示例
 * 在hive中使用 UDF需要在hive中注册
 *  1. 将UDF上传到服务器
 *  2. 进入hive模式
 *  3. 执行 add jar /xx/xx.jar
 *  4. 创建临时函数
        hive> create temporary function getNation as 'cn.itcast.hive.udf.NationUDF';
    5.调用
        hive> select id, name, getNation(nation) from beauty;

    6.将查询结果保存到HDFS中
        hive> create table result row format delimited fields terminated by '\t' as select * from beauty order by id desc;
        hive> select id, getAreaName(id) as name from tel_rec;
        create table result row format delimited fields terminated by '\t' as select id, getNation(nation) from beauties;
 */
public class NationUDF extends UDF {

    private static Map<String, String> nationMap = new HashMap<String, String>();

    static {
        nationMap.put("China", "中国");
        nationMap.put("Japan", "日本");
        nationMap.put("USA", "美帝");
    }
    Text t = new Text();
    //返回值为多个是 封装为一个对象返回
    //自定义类中 必须添加evaluate方法
    public Text evaluate(Text nation) {
        String nation_e = nation.toString();
        String name = nationMap.get(nation_e);
        if(name == null){
            name = "火星人";
        }
        t.set(name);
        return t;
    }
}
