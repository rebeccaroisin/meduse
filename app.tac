from twisted.application import internet, service
from meduse.protocol import MeduseFactory

application = service.Application("echo")
echoService = internet.TCPServer(8080, EchoFactory())
echoService.setServiceParent(application)