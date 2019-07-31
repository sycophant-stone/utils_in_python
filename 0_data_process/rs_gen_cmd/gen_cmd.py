
def gen_cmd(srcdat='rs.dat', cmdfile='rs.sh'):
    basestr = "curl 'http://taskmanage.aibee.cn/task/prod-training/submit_task/'  -d'name=fdpv3hu-redstar-sh-jq-20190729&env=prod&version=v190729a&extra_task_name=t1&store_config_file=fdp/unified_config/common/face/store_config/redstar.yaml&collect_metrics=True&use_job_executor=1&priority=97' &"
    with open(srcdat, 'r') as f:
        for line in f.readlines():
            words = line.strip().split('_')
            mallname = words[4]
            mallplace= words[5]
            start1 = basestr.find('sh')
            start2 = basestr.find('jq')
            newcmd = basestr[:start1]+mallname+'-'+mallplace+basestr[start2+2:]
            #print("%s-%s"%(mallname, mallplace))
            print(newcmd)
            '''
            pid = words[1]
            faceid = words[0]
            if pid not in pid_ptk_map:
                pid_ptk_map[pid] = set([])
            pid_ptk_map[pid].add(faceid)
            '''




if __name__ == '__main__':
    gen_cmd(srcdat='rs.dat', cmdfile='rs.sh')
