Flink Controller 测试程序
-------

## 环境配置

Java: JDK8及以上

Flink集群: 1.9（单机模式不需要安装Flink集群）

操作系统：理论上不限


## 打包

进入项目根目录，使用maven打包：

```
mvn clean package -Dmaven.test.skip
```

打包结束后，项目根目录下会产生Target目录，flink_controller/host-security/target

该目录下,会存放 host-security-1.0-SNAPSHOT.jar的编译包

把这个包提交到flink上即可使用