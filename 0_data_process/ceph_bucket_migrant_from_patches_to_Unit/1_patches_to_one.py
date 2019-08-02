import argparse
import os
from multiprocessing import Pool

import boto
import boto.s3.connection
from boto.s3.key import Key

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from PIL import ImageFile

import fdp.utils.ceph as ceph
import traceback

ceph_client = ceph.Client()

ceph_config = dict(
        #bj=dict(
        #    access_key='U0K5O8KP6P8RQZQV333U',
        #    secret_key='PuIp09FCBmeoPfXy0SCCjy7bgt3G7xG1BCZytMPM',
        #    host='s3.aibee.cn',
        #),
        bj=dict(
            access_key='4KXXWD2US79O41SUB8KE',
            secret_key='1wJb6LotmaKKiIdCrJOFACIwmjrtJnXqWOI9qeMf',
            host='s3-prod.aibee.cn',
        ),
        sh=dict(
            access_key="C7YP5KC0QLZRO2ZG15X2",
            secret_key="tQ609BhT2wL2MH8JRoaBdx0dAsAtui03dclRJV6C",
            host='172.19.14.8',
        ),
        gz=dict(
            access_key="SG5JMQ10LSMCJVNJW65O",
            secret_key="187t7PPQABNr3C1xaAOLND1XmZSQxZ6h1HEyPs3n",
            host='gz-s3.aibee.cn',
        ),
    )
def bucket_exists(conn, candidate_name):
    for bucket in conn.get_all_buckets():
        if candidate_name == bucket.name:
            print("candidate :%s   >>>> bucket name :%s"%(candidate_name, bucket.name))
            return True
    return False

def get_or_create_bucket(conn, name):
    if bucket_exists(conn, name):
        face_bucket= conn.get_bucket(name)
    else:
        face_bucket = conn.create_bucket(name)  
    return face_bucket

def create_new_bucket_name_by_obname(old_bucket_name):
    print("old_bucket_name:",old_bucket_name)
    if 'wanda' not in old_bucket_name and 'k11' not in old_bucket_name and 'redstar' not in old_bucket_name and 'ctf' not in old_bucket_name:
        print("old bucket name SANITY check fail")
        return None
    else:
        new_bucket_name = old_bucket_name.split('_')[-1]
        if len(new_bucket_name)!=8:
            print("old bucket name SANITY check fail")
            return None
        face_bucket_name = 'face_prod_daily_data_%s'%(new_bucket_name)
        return  face_bucket_name
           
def migrant_to_new(conn, old_bucket_list, one_tst_key_name=""):
    for obname in old_bucket_list:
        old_bucket_name = obname
        new_bucket_name = create_new_bucket_name_by_obname(old_bucket_name)
        if new_bucket_name==None:
            raise Exception("new bucket name is invalid")
            return None
        print('CURRENT old BUCKET name :%s'%(obname))
        print('get or create ob, nb ...')
        ob = conn.get_bucket(obname)
        nb = get_or_create_bucket(conn, new_bucket_name)
        print('... done')

        print('loop for old key ...')
        for old_key in ob.list():
            try:
                if old_key is None:
                    raise Exception('The key %s is empty in %s...' % (key.name, b.name))
                    return None
                #print('old key name :%s'%(old_key.name))
                if one_tst_key_name and one_tst_key_name!=old_key.name:
                    continue
                if one_tst_key_name:
                    print('get one test key name :%s'%(old_key.name))
                #print('old key name :%s'%(old_key.name))
                download_specific_key_and_bucket(conn,old_key,ob)
                new_key_name = old_key.name
                new_key = nb.new_key(new_key_name)
                new_key.set_contents_from_string(old_key.get_contents_as_string())
                if one_tst_key_name:
                    download_specific_key_and_bucket(conn,new_key,nb)
                    return None
            except Exception as e:
                print('old bucket name:%s, old key name:%s, new bucket name:%s with e:%s ' % (ob.name, old_key.name, nb.name, e))
                traceback.print_exc()
                return None
        print('... done')

def get_bucket_list_from_file(bucket_list_path):
    if not os.path.exists(bucket_list_path):
        raise Exception("%s not Exists"%(bucket_list_path))
        return 
    bucket_list = []
    with open(bucket_list_path, 'r') as f:
        for line in f.readlines():
            words = line.split('\t')
            print(words)
            bucket_name = words[0]
            bucket_list.append(bucket_name)
    return bucket_list

def download_specific_key_and_bucket(conn, key, bucket):
    if not os.path.exists(bucket.name):
        os.mkdir(bucket.name)
    img = Image.open(BytesIO(key.get_contents_as_string()))
    img.save("%s/%s.jpg"%(bucket.name, key.name))

ONE_TST = False 
if __name__ == '__main__':
    #main(oneshot_bn = "afu_beijing_cytj_20190501", wtest=False)
    idc = 'bj'
    conn = boto.connect_s3(
            aws_access_key_id = ceph_config[idc]['access_key'],
            aws_secret_access_key = ceph_config[idc]['secret_key'],
            host = ceph_config[idc]['host'],
            is_secure=False,               # uncomment if you are not using ssl
            port=80,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )
    if ONE_TST:
        old_bucket_list_tmp = get_bucket_list_from_file("one_test_bucket.list")
        print(old_bucket_list_tmp)
        migrant_to_new(conn, old_bucket_list = old_bucket_list_tmp, one_tst_key_name="-WANDA-bj-tzwd-20190724-ch00012_20190724092953.mp4.cut.mp4-ch00012_20190724092953.mp4.cut.mp4-0-0001296_0.980543")
        download_specific_key_and_bucket
    else:
        gold_bucket_list_tmp = et_bucket_list_from_file("old_bucket.list")
        migrant_to_new(conn, old_bucket_list = old_bucket_list_tmp, one_tst_key_name="")
       
