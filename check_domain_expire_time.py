#!/usr/bin/python3.6
import datetime
import socket
import ssl
import os


def ssl_expiry_datetime(hostname):
    ssl_date_fmt = r'%b %d %H:%M:%S %Y %Z'

    context = ssl.create_default_context()
    conn = context.wrap_socket(
        socket.socket(socket.AF_INET),
        server_hostname=hostname,
    )
    try:
        # 3 second timeout because Lambda has runtime limitations
        conn.settimeout(3.0)
        conn.connect((hostname, 443))
        ssl_info = conn.getpeercert()
    finally:
        if conn:
            conn.close()

    # parse the string from the certificate into a Python datetime object
    return [hostname, datetime.datetime.strptime(ssl_info['notAfter'], ssl_date_fmt)]

def main():
    domainlistfile=input("请输入检测域名文件:").strip()
    if os.path.isfile(domainlistfile) and  os.access(domainlistfile, os.R_OK):
        pass
    else:
        print('文件不存在或文件无读取权限')
    domainlist=[]
    with open(domainlistfile, 'r') as f:
        filelines = f.readlines()
    for i in filelines:
        if 'server_name' in i or ';' in i:
            pass
        else:
            domain = i.strip()
            #domainlist.append(getdnsresolver(domain)   
            domainlist.append(ssl_expiry_datetime(domain))
    for j in domainlist:
        print('domain : {0:<25} expire_time:{1:^20}'.format(j[0], j[1].strftime('%Y%m%d-%H:%M:%S')))

if __name__ == '__main__':
    main()
