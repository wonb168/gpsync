# 加# %%方便单步调试，正式执行时替换为##%%，需要单步时还原为# %%
# %%
import os
import re 

# %%
# 导出dump文件并预处理
def dump(dbinfo,db,filename):
    cmd=f"export PGPASSWORD={dbinfo['pwd']} && pg_dump -h {dbinfo['host']} -p {dbinfo['port']} -U {dbinfo['usr']} -s -f {filename} -d {db}"  
    print('dump command:',cmd)
    os.system(cmd)
    # 替换）前的单空格
    cmd=r"sed -i 's/ )/)/g' "+filename
    print('replace " )" to ")":',cmd)
    os.system(cmd)

# %%
# 拆分dump文件
def split_dump(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    statements = []
    statement = ''
    in_function = False
    for line in lines:
        # line = line.strip() 
        # 去空行
        if line == '\n':
            line = line.strip("\n")
        if line.startswith('--'):
            continue
        else:
            statement += line
        if re.match(r'^CREATE FUNCTION', line):
            in_function = True
        if in_function:
            if re.search(r'\$_\$;$', line) or re.search(r'\$\$;$', line): #$_$;或$$;结尾
                statements.append(statement)
                statement = ''
                in_function = False
        else:
            if re.search(r';$', line):
                statements.append(statement)
                statement = ''
    return statements

# %%
# 处理目标库中多余的（源库删除或修改）
"""
    CREATE FUNCTION，drop
    CREATE TABLE ，drop
    ALTER ... OWNER TO
    CREATE EXTERNAL TABLE gpfdist外部表，暂不考虑
    COMMENT ON COLUMN
    REVOKE FROM
    GRANT FROM
"""
def del_dest(dump_dest,dump_src):
    dels=[elem for elem in dump_dest if elem not in dump_src]
    print('need del in dest db:',len(dels))
    for o in dels:
        print(o)
        if (obj:=re.findall(r'^CREATE TABLE (\S+)', o)):
            # print(obj)
            sql=f'drop table {obj[0]};'
            print(sql)
        if (obj:=re.findall(r'^CREATE FUNCTION (.*?\))', o)):
            # print(obj)
            sql=f'drop function {obj[0]};'
            print(sql)
    return dels

# %%
# 源库多的（源库新增或修改）
def src_adds(dump_src,dump_dest,dels):
    dump_dest2=list(set(dump_dest)-set(dels))
    print(len(dump_dest2))#16559

    new=[elem for elem in dump_src if elem not in dump_dest2]
    print('need add in dest db:',len(new))#580
    for o in new:
        print(o)

# %%
def main(dbinfo,dbinfo2,dbname):
    filename=dbname+'_src.sql'
    filename2=dbname+'_dest.sql'
    dump(dbinfo,dbname,filename)
    dump(dbinfo2,dbname,filename2)

    dump_src=split_dump(filename)
    print(len(dump_src))
    dump_dest=split_dump(filename2)
    print(len(dump_dest)) 

    dels=del_dest(dump_dest,dump_src)
    src_adds(dump_src,dump_dest,dels)  
    
# %%
if __name__='__main__':
    dbname='mdmaster_bsgj_dev551_product_dev'
    dbinfo={"host":"192.168.200.101", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    dbinfo2={"host":"192.168.200.73", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    main(dbinfo,dbinfo2,dbname)

# %%








