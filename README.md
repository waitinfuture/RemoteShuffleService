# 分支说明

目前有两个开发分支:
+ master              hdfs版本
+ local-storage       local版本

其中local-storage有最新代码，后续将会将两个分支合并

# 编译命令
EMR Spark-2.4.5 编译命令:

    ./dev/make-distribution.sh
    
Apache Spark-2.4.5 编译命令:

    ./dev/make-distribution.sh -Papache
    
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