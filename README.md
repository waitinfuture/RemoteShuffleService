# 分支说明

# 编译命令

Tez编译

    mvn install -DskipTests -Dhadoop.version=3.1.3 -Pspark-3 -Ptez
    
# 包说明
编译生成一个压缩包ess-1.0.0-release.tgz,解压目录如下:

    ├── RELEASE
    ├── bin
    ├── conf
    ├── jars
    ├── sbin
    └── spark
