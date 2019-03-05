#!/usr/bin/python2.7
#-*- encoding:utf-8 -*-
#python 分析慢SQL
import hashlib 
import copy
import json
import datetime

class Apart(object):
    def __init__(self, filename, blocksize = 1 * 512 * 1024, position = 3996995332, point_end_position =0, querytime = 0.05 ,keyword = '# User@Host', outputfile = '/tmp/output.txt'):
        self._filename = filename
        self._position = position
        self._keyword = keyword
        self._blocksize = blocksize
        self._querytime = querytime
        self._fileobject = open(self._filename, 'r')
        self.positionmax()
        if point_end_position == 0:
            self._given_end_position = self._positinmax
        else:
            self._given_end_position = point_end_position
        self.get_read_block()
        self._md5 = hashlib.md5()
        self._md5_str = ''
        self._dict = {'MD5NUMBER':['ALL QUERY TIME', 'QUERY TIMES COUNT', ['MAX QUERY TIME','POSITION'], ['LOCK TIME', 'POSITION'], ['Rows_examined', 'POSITION'], 'SQL CONTENT', 'From Host' ] }
        self._total_list = []
        self._sql_str = ''
        self._str_slice = ''
        self._str_time_list = []
        self._host = ''
        self._time = ''
        self._global_point = 0
        self._str_querytime = 0.0
        self._str_locktime = 0.0
        self._str_Rows_examined = 0
        self._dict_list = []
        self._outputfile = outputfile
    def positionmax(self):
        self._fileobject.seek(0,2)
        self._positinmax = self._fileobject.tell()
        self._fileobject.seek(self._position,0)
    @property
    def position(self):
        return self._position
    @position.setter
    def position(self, value):
        if not instance(value, int):
            raise ValueError('position must be an ineger !')
        if value > self._positinmax:
            raise ValueError('position value is more than positionmax value')
        self._position = value
    def get_read_block(self):
        self._readstr = self._fileobject.read(self._blocksize)
        self._str_startposition = self._readstr.find(self._keyword)
        self._str_point_position = self._str_startposition
        self._str_end_position = self._readstr.rfind(self._keyword)
        if self._str_end_position == 0:
            print 'Read file finish, last content check:'
            print '-'*50
            print '{0}'.format(self._readstr)
            print '-'*50
            self._position = self._fileobject.tell()
            self._fileobject.close()
            raise IOError('finish Read ')
        if len(self._readstr) < self._blocksize:
            print 'Read file content Finished !'
            self._blocksize = len(self._readstr)
            #raise IOError('finish Read ')
        try:
            self._fileobject.seek(-1 * (self._blocksize - self._str_end_position), 1)
            self._position = self._fileobject.tell()
            #print 'DEBUG', 'try 语句中_postion 位置', self._position
        except IOError:
            print 'file.seek({},{})'.format(-1 * (self._blocksize- self._str_end_position), 1)
            self._fileobject.close()
            exit(1)
        if self._position > self._given_end_position:
            print "Read file finish, have come to the pointed end position"
            raise IOError('finish Read ')
    def read_slice(self):
        def _make_it_simple():
            try:
                self._str_time_list = int(self._total_list[-3].split('=')[1])
                self._time = datetime.datetime.fromtimestamp(self._str_time_list).strftime('%Y-%m-%d %H:%M:%S')
            except:
                self._time = 'unkonwn, please check original file'
        try:
            self._total_list = self._str_slice.split(';')
            self._sql_str = self._total_list[-2]   # incase there is ';' like 'use database;'
            self._sql_str_slice = self._sql_str.split('WHERE')[0].split('where')[0]
            self._md5 = hashlib.md5()
            self._md5.update(self._sql_str_slice)
            self._md5_str = self._md5.hexdigest()
            """
            # 调试使用
            if self._md5_str == '0d05e723673ec784a0cceaa69fa5c15c':
               self.sync_file(self._sql_str) 
            """
            #_str_time_list # 分割一行内容切为list
            self._str_time_list = self._total_list[0].split('#')[1]
            self._host = self._str_time_list.split('[')[2].split(']')[0]
            if self._host == '':
                self._host = 'localhost dbbak'
            self._str_time_list = self._total_list[0].split('#')[3].split(' ')
            self._str_querytime = float(self._str_time_list[2])
            self._str_locktime = float(self._str_time_list[5])
            self._str_Rows_examined = float(self._str_time_list[11])
            self._global_point = self._position - self._str_end_position + self._str_point_position 
        except:
            print 'DEUBG:', self._total_list, self._str_slice  
            print '-'*50
            print type(self._md5), self._md5.hexdigest(), 'xxx', self._md5_str
            print '-'*50
            print self._str_time_list, self._str_querytime, self._str_locktime 
            raise ValueError('read_slice error')
        # querytime 制定过滤满查询时间
        if self._str_querytime > self._querytime:
            if self._dict.has_key(self._md5_str):
                self._dict_list = copy.deepcopy(self._dict[self._md5_str])
                self._dict_list[0] += self._str_querytime
                self._dict_list[1] += 1
                if self._dict_list[2][0] < self._str_querytime:
                    #self._str_time_list = int(self._total_list[-3].split('=')[1])
                    #self._time = datetime.datetime.fromtimestamp(self._str_time_list).strftime('%Y-%m-%d %H:%M:%S')
                    _make_it_simple()
                    self._dict_list[5] = self._sql_str
                    self._dict_list[2][0] = self._str_querytime
                    self._dict_list[2][1] = self._global_point
                    self._dict_list[2][2] = self._time
                if self._dict_list[3][0] < self._str_locktime:
                    #self._str_time_list = int(self._total_list[-3].split('=')[1])
                    #self._time = datetime.datetime.fromtimestamp(self._str_time_list).strftime('%Y-%m-%d %H:%M:%S')
                    _make_it_simple()
                    self._dict_list[3][0] = self._str_locktime
                    self._dict_list[3][1] = self._global_point
                    self._dict_list[3][2] = self._time
                if self._dict_list[4][0] < self._str_Rows_examined:
                    #self._str_time_list = int(self._total_list[-3].split('=')[1])
                    #self._time = datetime.datetime.fromtimestamp(self._str_time_list).strftime('%Y-%m-%d %H:%M:%S')
                    _make_it_simple()
                    self._dict_list[4][0] = self._str_Rows_examined
                    self._dict_list[4][1] = self._global_point
                    self._dict_list[4][2] = self._time
                self._dict[self._md5_str] = self._dict_list
            else:
                #try:
                #    self._str_time_list = int(self._total_list[-3].split('=')[1])
                #except:
                #    print self._total_list
                #    print self._total_list[-3]
                #    print self._total_list[-3].split('=')[1]
                #    print 'xxxxxxxxxxx==========================='
                #    print self._global_point
                #    exit(10)
                _make_it_simple()
                self._dict_list = [self._str_querytime, 1, [self._str_querytime, self._global_point, self._time], [self._str_locktime, self._global_point, self._time], [self._str_Rows_examined, self._global_point, self._time], self._sql_str, self._host]
                self._dict[self._md5_str] = self._dict_list
        else:
             #if self._dict.has_key(self._md5_str):
             #   self._dict_list[1] +=1
             pass
    def get_slice(self):
        if self._str_point_position == self._str_end_position:
            self.get_read_block()
        self._str_nextposition = self._readstr.find(self._keyword, self._str_point_position+1)
        self._str_slice = self._readstr[self._str_point_position:self._str_nextposition]
        #print 'DEBUG', 'get slice ', 'str_point_position:', self._str_point_position
        self.read_slice()
        self._str_point_position = self._str_nextposition
        #print self._dict_list
        return self._str_slice
    def __iter__(self):
        return self             #将自生构建为迭代对象，返回自己
    def next(self):
        try:
            return self.get_slice()
        except IOError:
            raise StopIteration();
    def sync_file(self, line):
        with open(self._outputfile, 'a+') as f:
            f.write(line)
            f.write('\n')
    def standoutput(self):
        list_sort = []
        for i,j in self._dict.iteritems():
            list_sort.append((i,j[2][0]))
        list_sort = sorted(list_sort, lambda x, y: cmp(y[1], x[1]))
        self.sync_file('当前检测文件:' + self._filename)
        for k in list_sort:
            if self._dict[k[0]][6] == 'localhost dbbak':continue
            if k[0] != 'MD5NUMBER':
                value_list = self._dict[k[0]]
                self.sync_file('*'*140)
                self.sync_file('ID:' + str(k[0]) )
                self.sync_file('来源主机: ' + str(value_list[6]))
                self.sync_file('查询次数: ' + str(value_list[1]))
                self.sync_file('平均查询时间: ' + str(value_list[0] / value_list[1]))
                self.sync_file('最大查询时间: ' + str(value_list[2][0]) + '\t\t' + '文件记录位置: ' + str(value_list[2][1]) + '\t\t' + '执行开始时间: ' +  str(value_list[2][2]))
                self.sync_file('最大加锁时间: ' + str(value_list[3][0]) + '\t\t' + '文件记录位置: ' + str(value_list[3][1]) + '\t\t' + '执行开始时间: ' +  str(value_list[3][2]))
                self.sync_file('最大检查行数: ' + str(value_list[4][0]) + '\t\t' + '文件记录位置: ' + str(value_list[4][1]) + '\t\t' + '执行开始时间: ' +  str(value_list[4][2]))
                self.sync_file('SQL语句实例:: ' + str(value_list[5]) )
                #print value_list
    def run(self):
        for i in self:
            pass
        print 'self._dict  length is :',len(self._dict)
        self.standoutput()
#atest = Apart('/data/DATA/DBAA/server-logs/mysql-slow.log-20170807', position = 0 , querytime = 0.0 , outputfile='/home/dendi/20170807.mysql.slow.log')
#file_name='/data/DATA/DBAA/server-logs/mysql-slow.log-20170915'
file_name='/data/DATA/DBAA/server-logs/mysql-slow.log'
file_name='ss1.txt'
log_file = '/home/dendi/{0}.mysql.slow.log'.format(datetime.datetime.now().strftime("%Y%m%d-%H%M"))
#atest = Apart(file_name, position = 5786708606 , point_end_position =6336810969, querytime = 0.1 , outputfile=log_file)
atest = Apart(file_name, position = 0 , point_end_position = 0 , querytime = 0.1 , outputfile=log_file)
#atest = Apart('/data/slow-sql/1.log', position = 0 , querytime = 0.0)
print atest.position
print atest._positinmax
#print atest.get_slice()
atest.run()
