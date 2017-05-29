# hd-client
it is a client for hadoop components,but up to now,it is only a hbase client.
参考项目：[orm-hbase](https://github.com/zacharyzhanghao/orm-hbase)

# Client
- HbaseClient:基于注解来确定Column Metadata，然后映射为对象。支持复杂对象映射（序列化映射以及拆分映射），支持Java集合映射（序列化）
- HbaseTemplate:与Spring Hadoop提供一样的API，但更加高效。对比版本为Spring Hadoop 2.4。

# 使用方法
请注意：此项目强依赖于Spring，无法脱离Spring使用。
[详见单元测试](https://github.com/Paleozoic/hd-client/tree/master/src/test/java/com/maxplus1/test/hbase)

# 扩展
- 使用不同的序列化工具：只需要实现`HbaseSerializer`接口即可，然后在配置文件注入序列化器。
- 扩展类型处理：只需要在`resolvers`包下添加新的`TargetTypeResolver`子类，然后在`TypeResolverFactory`添加新类型的处理逻辑。

# TODO
- 文档逐步完善
- 设计思路整理
- 增加多表关联的ORM查询以及插入
- 降低Spring依赖，部分注入处使用单例模式
