greenplum集群本身的segment有primary和mirror，自动同步。但2个gp集群之间没有现成的同步方案，于是DIY一个。
# 前提假设
主备集群除了IP、规模（节点数）可能不一样外，其他配置都应该一样，如表空间、插件等。
# 总体思路
1. 要减少数据量的传输，尽量使用增量同步
2. 速度要快，要使用并行
3. 尽量减小对主库的压力，比对操作不宜频繁查询主库的元数据表。

增量同步时先同步结构，再同步数据。
## 同步结构
1. 比对差异
pg_dump导出主、备库的结构（不含数据）成dump sql文件。
比对2份dump sql文件，拆分sql（每条sql一个数据库对象），找出差异部分：新增、删除和修改3种情形。
2. 同步差异
原本以为表特殊，但实际上表增减字段后，基本上数据都要重算，所以原数据也没有意义了，需要从主表同步新的。
这样，表和其他对象都一致了，有差异的部分都可以drop掉备库的，执行主库的sql。

## 同步数据
能并行的gp迁移方案只有gpfdist（封装后为gpload）和gpcopy，gpcopy更优。
同时gpcopy的json配置文件可以写sql，这样可以控制sql的where条件实现增量copy。未配置的默认全量。

## 使用方法
目前只有1个gpsync.py文件和1个config.toml文件。调用时只需传入源和目标库的连接信息及数据库名。以gpadmin最高权限运行，方便运行多个库即授权。
```
if __name__=='__main__':
    dbname='mdmaster_platform'#'mdmaster_bsgj_dev551_product_dev'
    dbinfo={"host":"192.168.200.207", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    dbinfo2={"host":"192.168.200.73", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    print('begint to sync schema...')
    sync_schema(dbinfo,dbinfo2,dbname)
    print('begint to sync data...')
    copy_data(dbinfo,dbinfo2,dbname)
```
