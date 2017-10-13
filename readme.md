## 说明
1. Q：单行数据最多多少字段，拼在一起多少长度  
A：无限制字段、长度，只要数据能存进数据库
2. Q：单表数据最多支持多少行（数量级）  
A：可能是最大表空间，也可能是Oracle的最大数量级
3. Q：是否包括LOB类型的数据对比  
A：是

## 部署及运行
1. 安装Python3.5.4

    ```
    su - root
    tar -zxvf Python-3.5.4.tgz
    cd Python-3.5.4
    export LANG=zh_CN.UTF-8
    export LANGUAGE=zh_CN.UTF-8
    ./configure
    make
    make install
    ```
2. 安装cx_Oracle
    ```
    su - root
    tar -zxvf cx_Oracle-6.0.2.tar.gz
    cd cx_Oracle-6.0.2
    python3 setup.py build
    python3 setup.py install
    ```
3. 部署程序
    ```
    su - oracle
    mkdir test
    路径可以自定义
    cd test
    上传generate.py, compare.py, config.ini至当前目录下
    修改config.ini文件中的database(比如本地数据库为orabiz,据库为tnsnames.ora中的配置)
    修改数据库的username和password
    ```
4. 运行
    ```
    su - oracle
    cd /home/oracle/test
    python3 generate.py xxx.json
    在另一个数据库中执行完全相同的操作
    得到2个result.json文件后，与compare.py放在同一路径下
    执行python3 compare.py xxx1.json xxx2.json
    最后得到输出DifferentTable.csv
    ```
5. 注意  
    ```
    generate.py的运行目录在第一个数据库比如是/home/oracle/test
    那么在第二个数据库运行时也一定要保证运行目录相同，否则会导致特征值计算错误
    生成的error.log为exp运行错误时的出错信息，如果不为空证明在执行exp时发生了错误，请按log排查
    ```
## 设计
1. generate.py设计思路：针对表的数据对比，以表为单位，使用Oracle数据库的exp导出命令，使用多进程执行exp命令后生成dmp文件，任意一个dmp文件生成后立即进行sha1值的计算，计算完毕立即删除文件，其中dmp的文件头包含时间等，需要动态跳过这些字符。对于包含类LOB类型的表进行特殊处理，读取表中所有数据并写入到文件中，生成完毕立即计算sha1值，计算完毕立即删除。最后，将得到的特征值字典序列化为json文件。
2. compare.py设计思路：将得到的json文件反序列化为dict，然后转化为set，最终结果=2个集合的并集-2个集合的交集，所以最终csv文件中出现的是2个数据库不同的表和对方没有的表*
3. PS：理论上不仅仅对比数据，还会对比表的结构；程序使用exp对session消耗很小


![image](C:/Users/alons/Desktop/1.jpg)


