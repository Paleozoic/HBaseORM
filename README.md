# hd-client
it is a client for hadoop components,but up to now,it is only a hbase client.
参考项目：[orm-hbase](https://github.com/zacharyzhanghao/orm-hbase)

# Client
- HbaseClient:基于注解来确定Column Metadata，然后映射为对象。支持复杂对象映射（序列化映射以及拆分映射），支持Java集合映射（序列化）
- HbaseTemplate:与Spring Hadoop提供一样的API，但更加高效。对比版本为Spring Hadoop 2.4。
- MapClient：不建议使用，会扫描全行，对于超多列的表性能受到影响。我写出来是为了做演示项目。但这个MapClient有与SQL结合的潜力。

# 使用方法
[详见单元测试](https://github.com/Paleozoic/hd-client/tree/master/src/test)

# TODO
- 文档逐步完善
- 设计思路整理
- 单元测试全覆盖
- 增加多表关联的ORM查询以及插入
- 降低Spring依赖，部分注入处使用单例模式
