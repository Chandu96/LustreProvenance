import os
import re
import subprocess
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ServiceUnavailable
from pathlib import Path

#url = "bolt://10.161.40.85:7687/"
url = "bolt://10.201.120.142:7687/"
password = "neo4j_usage"
driver = GraphDatabase.driver(url,auth=basic_auth("neo4j", password),encrypted=False)

def get_db():
        #if not hasattr(g, 'neo4j_db'):
        neo4j_db = driver.session()
        return neo4j_db


def close_db(error):
        #if hasattr(g, 'neo4j_db'):
        #g.neo4j_db.close()
        driver.session().close()

def create_nodes_rels_and_return():
        print("entered nodes rels ")
        db = get_db()
        person1_name = 'Alice'
        person2_name = 'Bob'
        query = (
            "CREATE (p1:Person { name: $person1_name }) "
            "CREATE (p2:Person { name: $person2_name }) "
            "CREATE (p1)-[:KNOWS]->(p2) "
            "RETURN p1, p2"
        )
        result = db.run(query, person1_name=person1_name, person2_name=person2_name)
        try:
                return [{"p1": row["p1"]["name"], "p2": row["p2"]["name"]}
                        for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
                raise


def create_nodes_rels_in_neo4j():
        result = create_nodes_rels_and_return()
        for row in result:
                print("Created friendship between: {p1}, {p2}".format(p1=row['p1'], p2=row['p2']))


def create_user_actions_in_neo4j(operation_type,timestamp,datestamp,target_fid,job_id,user_id,file_name,path,username,mode):
        print(operation_type +" "+timestamp+" "+datestamp+" "+target_fid+" "+job_id+" "+user_id+" "+file_name+" "+mode)
        db = get_db()
        query = (
                "MERGE (user:User { user_id: $user_id }) ON CREATE SET user.username = $username "
                "MERGE (process:Process {process_name: $process_name }) ON CREATE SET user.username = $username "
                "MERGE (user) - [:CREATED] -> (process) "
                "MERGE (file:File { name: $file_name }) ON CREATE SET file.target_fid = $target_fid, file.path = $path "
                "MERGE (process) - [:"+operation_type+" {timestamp: $timestamp, datestamp: $datestamp, mode: $mode}] -> (file) "
        )
        db.run(query, user_id=user_id,username=username,process_name=job_id,file_name=file_name,target_fid=target_fid,timestamp=timestamp,datestamp=datestamp,path=path,mode=mode)
		
def decode_changelogs(changelogs_list):
        for changelog in changelogs_list:
                mode = ''
                dirctory = False
                chlog = re.sub(' +',' ',changelog)
                chlog = chlog.split(' ')
                operation_type = chlog[1][2:]
                timestamp = chlog[2].split('.')[0]
                datestamp = chlog[3]
                target_fid = chlog[5].split('=')[1]
                job_id_result = chlog[6].split('=')[1]
                job_id = ".".join(job_id_result.split('.')[0:-1])
                user_id = chlog[8].split('=')[1].split(':')[0]
                log_columns = len(chlog)
                #print("op:"+operation_type+" time:"+timestamp+" date:"+datestamp+" target_fid:"+target_fid+" job_id:"+job_id+" u_id:"+user_id+" n_cl:"+str(log_columns))
                #if chlog[log_columns-1].
                cmd_fid_path = "lfs fid2path /lustre/test "+ target_fid
                #cmd_fid_path = 'lfs fid2path \\lustre\\test [0x200004a51:0x2b:0x0]'
                fid2path_results = os.popen(cmd_fid_path).read()
                if operation_type == 'ATIME':
                        operation_type = 'ACCESS'
                if operation_type == 'OPEN':
                        mode = chlog[10].split('=')[1]
                if len(fid2path_results) != 0:
                        file_name = fid2path_results.split('/')[-1]
                        #username
                        username_results = os.popen("getent passwd "+user_id).read()
                        username = username_results.split(':')[0]
                        #check file/directory
                        print("path :"+fid2path_results)
                        #if Path(fid2path_results).is_dir():
                        #       dirctory = True
                        create_user_actions_in_neo4j(operation_type,timestamp,datestamp,target_fid,job_id,user_id,file_name,fid2path_results,username,mode)



cmd_get_changelogs = 'lfs changelog mytest-MDT0000'
changelog_results = os.popen(cmd_get_changelogs).read()

#print(changelog_results)
#print("type of changelogs", type(changelog_results))
list_changelogs = changelog_results.splitlines()
print(len(list_changelogs))
#print(len(changelog_results.splitlines()))
decode_changelogs(list_changelogs)



                       
