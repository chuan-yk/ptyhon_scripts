#!/usr/local/python3.6.6/bin/python3
#-*- coding:utf-8 -*-
import sys
import socket
# Zabbix 使用telegram 发生报警信息, 主要解决 subject message 换行问题

def client_sender(buffer,target,port):

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # connect to our target host
        client.connect((target,port))

        # if we detect input from stdin send it 
        # if not we are going to wait for the user to punch some in

        if len(buffer):
            client.send(buffer.encode(encoding='utf_8', errors='strict'))
			
        # while True:
            # # now wait for data back
            # recv_len = 1
            # response = ""

            # while recv_len:
                # data     = client.recv(4096)
                # recv_len = len(data)
                # response+= data

                # if recv_len < 4096:
                    # break

            # print response, 

            # # wait for more input
            # buffer = raw_input("")
            # buffer += "\n"                        

            # # send it off
            # client.send(buffer)


    except Exception as err:
        # just catch generic errors - you can do your homework to beef this up
        log("[*] Exception! Exiting.")
        log('Error happen like {0}'.format(str(err)))

        # teardown the connection                  
    client.close()
		
def log(msgs):
    with open('/tmp/tgm-py.log','a+') as f:
        f.write(str(msgs))
        f.write('\n')

def main():
    target="13.75.122.XXX" # 仅指定IP 可访问
    port=8890
    #$1 concact_user; $2 subject ; $3 message
    try:
        if len(sys.argv) < 4:
            log('Error argv number')
        sendto=sys.argv[1]
        subject=sys.argv[2]
        message=sys.argv[3].replace(chr(10), '\\n')
        my_buffer='msg {0} "{1} {2}"\n'.format(sendto, subject, message)
    except Exception as e:
        log(str(e))
		
    #my_buffer='msg dendi test11 1 1 1 1 22 2 2\n'
    client_sender(my_buffer, target, port)	

if __name__ == "__main__":
    main()		
