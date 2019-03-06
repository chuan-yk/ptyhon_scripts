#!/usr/local/python27/bin/python2.7
# -*- coding:utf-8 -*-
# 定时扫描 sql执行情况，出现符合条件的慢查询进行kill， 针对严重超时的SQL语句，防止数据库负载飙升 
import sys
import os
import time
import atexit
from signal import SIGTERM 
import pymysql


class Daemon:
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
    
    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
    
        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
    
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    
    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        
        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """        

class Mydatabase(object):
    """
    define mysql connect 
    """
    def __init__(self, host, username, passwd ,  limit_time = 3, thread_running_number = 70, port=3306, logfile='/tmp/auto_kill_slow_query.log', kill_time_rule = {'172.30.44.xxx':10000, '172,30.45.xxx':20, 'default':3 } , default_loglevel = 5):
        self._host = host
        self._username = username
        self._passwd = passwd
        self._limit_time = limit_time
        self._loglevel = default_loglevel
        self._thread_running_number = thread_running_number
        self._port = port
        self._logfile = logfile
        self.get_db_connected()
        self._kill_time_rule = kill_time_rule

    def sync_log(self, log_string , loglevel=4):
        """
        write log: level: 0 - no log ; 1 Fatal Error ; 2 Error ; 3 info ; 4 normal ; 5 debug
        """
        if loglevel <= self._loglevel: 
            with open(self._logfile, 'a+') as f:
                f.write(time.strftime("%Y-%m-%d %H:%M:%S")+' '*3)
                f.write(log_string + '\n')
    def get_db_connected(self, is_reconnect = True):
        """
            try  connect DB 
            is_reconnect default is True , log shows reconnect , else show first time to connect 
        ERROR CODE: 
                2013, 'Lost connection to MySQL server during query'
                1045, u"Access denied for user %% (using password: YES)"
                2003, "Can't connect to MySQL server on %% (timed out)"
        """
        if is_reconnect:
            logword = 'Start connect '
        else:
            logword = 'Reconnect '
        try:
            self._db = pymysql.connect(host=self._host, user=self._username, password=self._passwd, port = self._port)
            self._cs = self._db.cursor()
            self.sync_log('[notice] {0} to db successful...'.format(logword), loglevel = 4)
        except pymysql.err.OperationalError, e:
            self.sync_log(str(e))
            self.sync_log('[Fatal Error] {0} to mysql db Failed'.format(logword) , 1)

    def get_query_result(self,sql):
        """ use sql query , then return result """
        self.sync_log('[info] execute SQL: {0} '.format(sql), 5)
        try:
            self._cs.execute(sql)
            query_result = self._cs.fetchall()
            self.sync_log('[info] function get_query_result, get query_result is {0}'.format(query_result), 5)
            return query_result
        except pymysql.err.OperationalError, e:
            if e[0] == 2013:
                return 'lostconnection'
            else:
                self.sync_log(str(e), 2)
                self.sync_log('[Fatal Error]  query could not get correct answer , sql is {}'.format(sql), 1)
        except:
            self.sync_log('[Error]  unkown problem , sql is {}'.format(sql))
    def kill_pid(self, pid_number):
        """
        kill  pid number ， if failure, return 'lostconnection'
        except : pymysql.err.InternalError: (1094, u'Unknown thread id: xxxx') 
        except : pymysql.err.OperationalError: (2013, 'Lost connection to MySQL server during query') 
        """

        kill_pid_sql = 'kill {0}'.format(pid_number)
        self.sync_log('function kill_pid {0}'.format(pid_number))
        try:
            self.sync_log('try to execute sql: {0}'.format(kill_pid_sql), 3)
            self._cs.execute(kill_pid_sql)
        except pymysql.err.OperationalError, e:
            if e[0] == 2013:
                self.sync_log(str(e), 3)
                self.sync_log('lost connection from database , re-kill later', 3)
                return 'lostconnection'
            else:
                self.sync_log(str(e), 2)
                self.sync_log('[Error] pymysql.err.OperationalError: unknown problem, when try to kill', 2)                
        except pymysql.err.InternalError, e:
            if e[0] == 1094:
                self.sync_log(str(e), 3)
                self.sync_log('pid number kill failure , maybe this pid {0} has been finished !'.format(pid_number), 3)
                return  'kill_ok'  # this condition same as kill successful
            else:
                self.sync_log(str(e), 2)
                self.sync_log('[Error] pymysql.err.InternalError: unknown problem, when try to kill', 2)
        except:
            self.sync_log('kill_pid  Unexcepted Error,...', 1)
        else:
            return 'kill_ok'
    def check_threads_running(self):
        """
        according to self._thread_running_number , decide kill or not
        show global status like "Threads_running"
        """
        thread_running_number_sql = 'show global status like "Threads_running"'
        sql_result = self.get_query_result(thread_running_number_sql)
        if sql_result == 'lostconnection':
            self.get_db_connected()
            sql_result = self.get_query_result(thread_running_number_sql)
        self.sync_log('当前threading running 查询结果 {0}'.format(str(sql_result)), 5)
        try:
            if int(sql_result[0][1]) > self._thread_running_number:
                #debug('查询线程数返回结果为 true')
                return True
            else:
                return False
                #debug('查询线程数返回结果为 False')
        except ValueError, e:
            self.sync_log(str(e), 2)
            self.sync_log('[Fetal Error]something Wrong , get sql results is {0}'.format(sql_result), 1)
            #exit(0)     #if thread_running_number could not get , this script will not work, quit now.

    def id_kill_rule(self, host, time):
        """
        According to self._kill_time_rule check decide if should be kill 
        """
        #self.sync_log('[info] start id_kill_rule {0} {1}'.format(host, time), 5)
        try:
            if host in self._kill_time_rule.iterkeys():
                if int(time) > int(self._kill_time_rule[host]):
                    self.sync_log('[info] Function id_kill_rule get host: {0} time: {1}, return True'.format(host, time), 4)
                    return True
                else:
                    self.sync_log('[info] Function id_kill_rule get host: {0} time: {1}, return False'.format(host, time), 4)
                    return False
            else:
                if int(time) > int(self._kill_time_rule['default']):
                    self.sync_log('[info] Function id_kill_rule get host: {0} time: {1}, return True'.format(host, time), 4)
                    return True
                else:
                    self.sync_log('[info] Function id_kill_rule get host: {0} time: {1}, return False'.format(host, time), 4)
                    return False
        #except KeyError, e:
        except:
            self.sync_log('[info] def id_kill_rule unexcepted Error, host={0}, time={1}'.format(host, time), 2) 
            return False

    def get_longtime_query_id(self):
        """"""
        query_id_sql = 'select *  from information_schema.`PROCESSLIST` where  info like "select%" and time > {0}'.format(self._limit_time)
        self.sync_log('[notice] Ready to execute sql: \'{0}\' '.format(query_id_sql))
        result_list = self.get_query_result(query_id_sql)
        if result_list == 'lostconnection':
            self.get_db_connected()
            result_list = self.get_query_result(query_id_sql)
        self.sync_log('[info] get result is {0}'.format(result_list), 5)
        self._id_list = []
        self._count = 0
        self._length = len(result_list)
        try:
            for sub_list in result_list:
                t_id = sub_list[0]                          # id  字段
                t_host = sub_list[2].split(':')[0]          # 来源主机
                t_time = int(sub_list[5])                   # 查询耗时
                self._count += 1
                self.sync_log('[info] 第{0}条： 获取时间、 主机字段{1}  {2}'.format(self._count , t_time, t_host), 5)
                if self.id_kill_rule(t_host, t_time):
                    self._id_list.append([t_id, sub_list])
            self.sync_log('[info] After  for function , get_longtime_query_id return result are: {0}'.format( str(self._id_list)), 5)
        except:
            self.sync_log('[info] *******============= get_longtime_query_id return result are: {0}'.format(str(id_list)), 5)
            self.sync_log('[Fetal Error]function get_longtime_query_id  Error happen in for circle ', 2)
        finally:
            #return [(527720, 'lottery', '10.46.50.22:47564', 'lottery', 'Query', 1, 'Sending data', 'SELECT\n\t\t`issue_no` as issueNo,lottery_id as lotteryId,predicted_time as predictedTime\n\t\tFROM\n\t\t`t_lottery_issue`\n\t\tWHERE `lottery_id` = 37\n\t\tand `date` >= date_sub(current_date(),interval 1 day)\n\t\tand open_code is null and\n\t\topen_state = 0 and error_state = 0\n\t\tAND\n\t\t`sell_end` < NOW()\n\t\tORDER\n\t\tBY `id` DESC\n\t\tLIMIT 5', 271, 0, 0)]
            self.sync_log('[info] before return id_list get_longtime_query_id return result are: {0}'.format(str(self._id_list)), 5)
            return self._id_list

    def test(self, st='测试'):
        debug(' self.test  print  this in self test   , test  =========={0}'.format(st))
    def main_run(self):
        """just do it"""
        if self.check_threads_running():
            long_time_query_id_list = self.get_longtime_query_id()
            self.sync_log('[info] Get long_time_query_id_list are: {0}'.format(str(long_time_query_id_list)), 5)
            self._count2 = 0
            for sub_id in long_time_query_id_list:
                self._count2 += 1
                try:
                    self.sync_log('[info] sub_id in long_time_query_id_list , current sub_id = {0}'.format(sub_id[0]), 5)
                    kill_result = self.kill_pid(int(sub_id[0]))
                    if kill_result == 'lostconnection' :
                        self.get_db_connected()
                        kill_result = self.kill_pid(int(sub_id[0]))
                    if kill_result == 'kill_ok':
                        self.sync_log('[notice] kill query SLOW sql Ok , SQL detail were : {0}'.format(sub_id))
                    else:
                        pass
                except:
                    self.sync_log('[Error] kill pid: {0} failed, slow sql is {1} '.format(sub_id[0], sub_id), 2)
                else:
                    self.sync_log('[notice] 当前KILL {0} 条查询'.format( self._count2), 3)
                    self.sync_log('[notice]slow sql is:' + '\n' + '{0}'.format(str(sub_id[1][7])), 3)
                    self.sync_log('-'*10 + '我是分界线' + '-'*10, 3)
        else:
            #self.sync_log('current threads running number is less than {0}, do nothing here...'.format(self._thread_running_number))
            pass


class MyDaemon(Daemon, Mydatabase):
    def __init__(self, host, username, passwd ,  thread_running_number = 70, port=3306, limit_time = 5, logfile='/tmp/auto_kill_slow_query.log', kill_time_rule = {'172.30.44.xxx':10000, '172,30.45.xxx':20,'default':3 }, default_loglevel = 5, pidfile='/root/sup_mysql.pid', stdin='/dev/null', stdout='/dev/null', stderr='/dev/null' , execute_frequency = 30): 
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self._host = host
        self._username = username
        self._passwd = passwd
        self._limit_time = limit_time
        self._loglevel = default_loglevel 
        self._thread_running_number = thread_running_number
        self._port = port
        self._logfile = logfile
        self.get_db_connected()
        self._kill_time_rule = kill_time_rule
        self._execute_frequency = execute_frequency

    def run(self):
        while True:
            self.sync_log('[info] Execute every {0} sec'.format(self._execute_frequency), 5)
            self.main_run()
            time.sleep(self._execute_frequency)

def debug(fstr):
    with open('/tmp/debug.log', 'a+') as f:
        f.write(fstr+'\n')
if __name__ == "__main__":
    host = '???????'
    username = 'devops'
    passwd = '??????????'
    mine_port = ??                       # 设置数据库端口
    mine_thread_num = 75                     # 触发检查的前提， thread_running 超过该数值
    mine_log_level = 5                      # 日志级别 1 - 5
    mine_limittime = 3                      #  查询时间超过多长的进行检查, 单位秒
    mine_kill_time_rule = {'172.30.44.xxx':100000, '172.30.44.xxxx':100000, '172.30.44.xxxx':35, '172.30.44.xxxx':1000, 'default':5 }  # 不同来源主机，定义不同的超时时间
    mine_exe_fre_time = 5                   # 每多长时间执行一次，单位 秒
    daemon = MyDaemon(host, username, passwd, 
            port = mine_port,
            limit_time = mine_limittime,
            thread_running_number = mine_thread_num, 
            logfile='/tmp/auto_kill_slow_query.log', 
            stdout = '/tmp/test.run.txt', 
            kill_time_rule = mine_kill_time_rule,
            default_loglevel = mine_log_level,
            execute_frequency = mine_exe_fre_time )
    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)      
