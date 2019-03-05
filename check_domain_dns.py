#!/bin/env python3
# -*- coding:utf-8 -*-

import socket
import dns.resolver
import os
def getdnsresolver(domain):
    Res = dns.resolver.Resolver()
    try:
        query_result = Res.query(domain)
        ip_address = ''
        for i in query_result.response.answer:
            for j in i:
                ip_address = j
        return [domain, ip_address]
    except dns.resolver.Timeout as  err:
        print('{0} check_domain_dns_records by query DNS server , query DNS time out'.format(domain))
        return [domain, 'unknown']                                                                                                        
    except Exception as err:        
        print(str('{} check_domain_dns_records by query DNS server ,'.format(domain))+str(err))
        return [domain, 'unknown']

def getremoteaddr(domain):
    try:
        return [domain, socket.gethostbyname(domain)]
    except socket.gaierror as  err:
        return [domain, 'unknown']

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
            #domainlist.append(getdnsresolver(domain))
            domainlist.append(getremoteaddr(domain))
    for j in domainlist:
        print('domain : {0:<25} record:{1:^20}'.format(j[0], j[1]))

if __name__ == "__main__":
    main()
