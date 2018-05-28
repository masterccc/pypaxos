#!/usr/bin/env python3

# run : ./paxos.py ip port

import socket
import sys
import random


PTC_CLI_ASK = "!p4x0s_cli_ask"
PTC_CLI_RELEASE_SRV = "!p4x0s_release"
PTC_PROCESS = "!p4x0s_processing"
PTC_STOP = "!p4x0s_stop"

server = sys.argv[1]
port = int(sys.argv[2])

s = socket.socket()

s.connect((server,port))

def menu():
	print("1 - Save random value")
	print("2 - Kill serv")
	print("2 - Quit")


while(True):
	menu()
	choice = input("Choice >")
	if(  choice == "1"):
		val = str(random.randint(1,100))
		request = PTC_CLI_ASK + " " + val
		print("Tell server to save value " + val)
		s.send(request.encode())
		data = s.recv(1024)
		print(">>> " + data.decode("utf-8"))

	elif(choice == "2"):
		print("Kill serv")
		s.send(PTC_STOP.encode())
		sys.exit(0)
	else:
		print("Disconnect from serv" + val)
		s.send(PTC_CLI_RELEASE_SRV.encode())
		sys.exit(0)
