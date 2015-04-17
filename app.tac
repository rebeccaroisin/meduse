import sys
import os


sys.path.append(os.path.split(os.path.abspath(__file__))[0])

from twisted.application import internet, service
from meduse.protocol import MeduseFactory

current = os.getcwd()
head, tail = os.path.split(os.path.abspath(current))
print head, tail

def conf():
	entries = [s.split() for s in file(os.path.join(head, "config.txt"), "r").read().strip().split("\n")]
	entries = dict([(s[0], ("127.0.0.1", int(s[1]), s[0])) for s in entries])

	port = entries[tail][1]
	del entries[tail]
	return entries.values(), port

entries, port = conf()

application = service.Application("echo")

fac = MeduseFactory(tail)
fac.others = entries


echoService = internet.TCPServer(port, fac)

echoService.setServiceParent(application)