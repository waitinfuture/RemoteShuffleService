# 分支说明

目前有两个开发分支:
+ master              hdfs版本
+ local-storage       local版本

其中local-storage有最新代码，后续将会将两个分支合并

# Spark版本说明
对于ae版本和非ae版本，SparkUtils和MapStatus是有区别的
1. EMR版本从2.4开始，是支持AE的
2. Apache版本从3.0开始支持AE
3. Spark 2.4和Spark 2.3的shuffle协议是兼容的

# 编译命令

EMR Spark-2.4.5 （AE） 编译命令:

    ./dev/make-distribution.sh
    
Apache Spark-2.4.5（默认非AE） 编译命令:

    1. 本地的maven setting.xml不能包含emr的repo
    2. ./dev/make-distribution.sh -Papache-spark -Pspark-2
    
Apache Spark-3.0.1 编译命令:

    ./dev/make-distribution.sh -Pspark-3
    
# 包说明
编译生成一个压缩包ess-1.0.0-release.tgz,解压目录如下:

    ├── RELEASE
    ├── bin
    ├── conf
    ├── jars
    ├── sbin
    └── spark
    
其中spark目录保存的是shuffle manager的jar