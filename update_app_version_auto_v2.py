#!/bin/python2.7
#-*- coding:utf-8 -*-
# auto deployment update file , and change appversion.json
import os
import datetime
import sys
import json

pub_dir = '/var/www/html/online/static/image/mc_upload_dir/'
config = {}
# ccc 项目
config['ccc'] = {'dst_dir':'/var/www/html/online/static/app/', 'bkdir':'/backup/online/'}
# dddd APP文件
config['dddd'] = {'dst_dir':'/var/www/html/online/dddd/static/app/mod/', 'bkdir':'/backup/dddd/'}
# zzzz APP 文件
config['zzzzz'] = {'dst_dir':'/var/www/html/online/static/app/zzzz/', 'bkdir':'/backup/online_zzzzz'}
# 更新增量文件，自动拼接版本号 
def cover_file(platform, version):
    update_new_file = 'update{:.3f}.wgt'.format(version+0.001)
    if os.path.isfile(os.path.join(pub_dir, update_new_file)): #检查文件是否存在
        print '\033[95m' + '检测{0}  update{1:.3f}.wgt'.format(platform, version+0.001) + "文件已上传到{0}".format(os.path.join(pub_dir, update_new_file)) + '\033[0m'
    else:
	print '\033[93m' + '检测{0} update{1:.3f}.wgt'.format(platform, version+0.001) + "文件未上传,更新未完成~" + '\033[0m'
        exit(1)
    #备份
    datestr = datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d_%H%M%S')
    os.system('/bin/cp -r {0} {1}'.format(config[platform]['dst_dir'], os.path.join(config[platform]['bkdir'],'app_bak_'+datestr)))
    print '\033[96m' + '/bin/cp -r {0} {1}'.format(config[platform]['dst_dir'], os.path.join(config[platform]['bkdir'],'app_bak_'+datestr)) + '\033[0m'
    print '\033[96m' + '/bin/cp -r {0} {1}'.format(os.path.join(pub_dir, update_new_file), os.path.join(config[platform]['dst_dir'], 'update', update_new_file)) + '\033[0m'
    os.system('/bin/cp -r {0} {1}'.format(os.path.join(pub_dir, update_new_file), os.path.join(config[platform]['dst_dir'], 'update', update_new_file)))
    print '\033[96m' + '平台{0} 更新文件{1} 发布完成~'.format(platform, update_new_file) + '\033[0m'
# 读取appversion.json 文件
def getversion(platform):
    jsonfile = os.path.join(config[platform]['dst_dir'], 'appVersion.json')
    with open(jsonfile, 'r') as f:
        json_str = f.read()
    json_dict = json.loads(json_str)
    #print "getversion debug ", jsonfile, float(json_dict['version']),json_dict
    return float(json_dict['version']),json_dict
# 更新主函数
def updateplt(platform, the_version=None):
    old_ver = getversion(platform)[0]	#默认不指定版本号，通过文件读取
    json_dict = getversion(platform)[1]
    if the_version:old_ver=the_version-0.001  #指定版本号,修改旧版本号为指定版本号 - 0.001
    cover_file(platform, old_ver)
    if not the_version:		#
        json_dict['version'] = '{:.3f}'.format(old_ver+0.001)
        with open(os.path.join(config[platform]['dst_dir'], 'appVersion.json'), 'w+') as f:
            f.write(json.dumps(json_dict))
        print '\033[96m' + '更新{0}'.format(platform) + '版本号已修改,新版本号为:' + json.dumps(json_dict) + '\033[0m'
    
# 执行接收参数过程
def main():
    if len(sys.argv) == 2:
        theargv = sys.argv[1]
        the_version = None
    elif len(sys.argv) >= 3:
        theargv = sys.argv[1]   # 指定平台
        try:
            the_version = float(sys.argv[2]) # 指定版本号， 用于跳版本更新
        except ValueError as e:
            print '\033[93m' + '可选参数$2={} 非合法版本号，请重新输入'.format(sys.argv[2]) + '\033[0m'
            exit(1)
    else:
        print '\033[93m'+'用法:  ' + sys.argv[0] + ' $1=[ccc|dddd|zzzz|all], 可选参数指定版本号 $2 = 1.235 ' + '\033[0m'
        exit(0)
    if theargv == 'ccc':
        updateplt(sys.argv[1], the_version)
    elif theargv == 'dddd':
        updateplt(sys.argv[1], the_version)
    elif theargv == 'zzzzz':
        updateplt(sys.argv[1], the_version)
    elif theargv == 'all':
        updateplt('ccc', the_version)
        updateplt('dddd', the_version)
        updateplt('zzzzz', the_version)
    else:
        print '\033[93m'+'用法:' + sys.argv[0] + ' $1=[ccc|dddd|zzzzz|all], 可选参数指定版本号 $2 = n.nnn ' + '\033[0m'

if __name__ == '__main__':
    main()
