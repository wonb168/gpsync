# 加# %%方便单步调试，正式执行时替换为##%%，需要单步时还原为# %%
# %%
import os,sys
import re 
import tomli
import json
import psycopg2
from pandas import json_normalize

# %%
# 由yaml配置文件生成include table的json、exclude table的txt以及include table的delete sql

def gen_copyfile(cfg_file='config.toml'):
    # exclude table file：db.schema.table
    with open(cfg_file, "rb") as f:
        toml_dict = tomli.load(f)
    df = json_normalize(toml_dict)#扁平化json数据
    # 写exclude文件
    str = '\n'
    print('generate exclude_table.txt...')
    with open("exclude_table.txt","w") as f:
        f.write(str.join(df.columns.tolist()))
    dct_json=[]
    dels=[]
    for r in df.columns.tolist():
        row=f'delete from {r} {df[r][0]};\n'
        # print(row)
        dels.append(row)
        dct={}
        dct['source']=r
        dct['sql']=f"select * from {r} {df[r][0]}"
        dct['dest']=r
        dct_json.append(dct)
    print('generate include_table.json...')
    with open("include_table.json","w") as f:
        f.write(json.dumps(dct_json))
    return dels

# %%
def run_sql(sql,dest,dbname):
    conn = psycopg2.connect(database=dbname, user=dest['usr'],	password=dest['pwd'], host=dest['host'], port=dest['port'])
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()
    
# %%
# use gpcopy to transfer data
def copy_data(source,dest,dbname):
    # 增量用json文件，同时先delete表数据
    dels='\n'.join(gen_copyfile())  
    print('delete in dest db:\n',dels)

    run_sql(dels,dest,dbname)
    cmd=f"""export PGSSLMODE=disable && export PGPASSWORD={source['pwd']} && gpcopy \
--source-host {source['host']} --source-port {source['port']} --source-user {source['usr']} \
--dest-host {dest['host']} --dest-port {dest['port']} --dest-user {dest['usr']} \
--include-table-json include_table.json --append
"""
    print(cmd)
    os.system(cmd)
    # 全量用exclude
    cmd=f"""export PGSSLMODE=disable && export PGPASSWORD={source['pwd']} && gpcopy \
--source-host {source['host']} --source-port {source['port']} --source-user {source['usr']} \
--dest-host {dest['host']} --dest-port {dest['port']} --dest-user {dest['usr']} \
--dbname {dbname} --exclude-table-file exclude_table.txt --truncate
"""
#     cmd="""export PGSSLMODE=disable && gpcopy --source-host {} --source-port {} --source-user {} \
# --dest-host {} --dest-port {} --dest-user {} \
# --dbname {} --exclude-table-file exclude_table.txt --truncate
# """.format(source['host'],source['port'],source['usr'],dest['host'],dest['port'],dest['usr'],dbname) 
    print(cmd)
    os.system(cmd)
    
    
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
def dest_has(dump_dest,dump_src):
    # 若表空间不一致，直接退出
    # compare_tablespace(dump_src,dump_dest)
    has=[elem for elem in dump_dest if elem not in dump_src]
    print('need del in dest db:',len(has))
    dels=[]
    for o in has:
        print(o)        
        if (obj:=re.findall(r'^CREATE TABLE (\S+)', o)):
            print(obj,len(obj))
            sql=f'drop table {obj[0]};'
            print(sql)
            dels.append(sql)
        if (obj:=re.findall(r'^CREATE FUNCTION (.*?\))', o)):
            sql=f'drop function {obj[0]};'
            print(sql)
            dels.append(sql)
        if (obj:=re.findall(r'^CREATE INDEX (\S+)', o)):
            sql=f'drop index {obj[0]};'
            print(sql)
            dels.append(sql)
        if (obj:=re.findall(r'^GRANT (.*+) TO (.*;)', o)):
            sql=f'revoke {obj[0]} from {obj[1]};'
            print(sql)
            dels.append(sql)
    # 目标库中执行dels
    return dest_has

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
def sync_schema(dbinfo,dbinfo2,dbname):
    filename=dbname+'_src.sql'
    filename2=dbname+'_dest.sql'
    dump(dbinfo,dbname,filename)
    dump(dbinfo2,dbname,filename2)

    dump_src=split_dump(filename)
    print(len(dump_src))
    dump_dest=split_dump(filename2)
    print(len(dump_dest)) 

    dels=dest_has(dump_dest,dump_src)
    src_adds(dump_src,dump_dest,dels)  
    
# %%
if __name__=='__main__':
    dbname='mdmaster_platform'#'mdmaster_bsgj_dev551_product_dev'
    dbinfo={"host":"192.168.200.101", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    dbinfo2={"host":"192.168.200.73", "port":2345, "usr":"gpadmin", "pwd":"密码"}
    print('begint to sync schema...')
    # sync_schema(dbinfo,dbinfo2,dbname)
    print('begint to sync data...')
    copy_data(dbinfo,dbinfo2,dbname)


# %%








