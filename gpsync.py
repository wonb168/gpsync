import sh,os
import re 
import pandas as pd
import duckdb as dd

# 导出数据库结构
def dump(dbinfo,db,source):
    cmd=f"export PGPASSWORD={dbinfo['pwd']} && pg_dump -h {dbinfo['host']} -p {dbinfo['port']} -U {dbinfo['usr']} -s -f {db}_{source}.sql -d {db}"  
    print(cmd)
    os.system(cmd)
    
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
  
  # list转表
  def list2df(list):
    return pd.DataFrame(list, columns=['sql'])
