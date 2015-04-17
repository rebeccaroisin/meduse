from twisted.internet import protocol, reactor

import random
import shelve

FOLLOWER = 1
CANDIDATE = 2
LEADER = 3

## Factories to connect to the other raft peers

class PeerClientFactory(protocol.ClientFactory):
    def __init__(self, upper_factory):
        self.factory = upper_factory

    def startedConnecting(self, connector):
        print 'Started to connect.'

    def buildProtocol(self, addr):
        print 'Connected.'
        return MeduseProtocol(self.factory)

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason

## An instance of the raft server

class MeduseProtocol(protocol.Protocol):


    def __init__(self, factory):
        self.factory = factory
        self.buffer = ""


    def dataReceived(self, data):
        ## When we receive data by a candidate or leader back off our own timer
        self.factory.reset_election_timeout()

        if data[0] == "RequestVote":
            # Parse incoming message
            _, term, candidateID, lastLogIndex, lastLogTerm = data

            if term > self.factory.current_term:

                self.factory.state = FOLLOWER
                self.factory.voted_for = None
                self.factory.set_persistant(term, None)


            # If old epoch, do not grant vote and send new epoch
            if term < self.factory.current_term:
                outmsg = (self.factory.current_term, False)
                self.transport.write(str(outmsg))
                return

            # If same epoch but we have already voted for someone
            (log_term, log_index, _) = self.factory.get_last_log()
            if self.factory.voted_for is None and \
                log_term <= lastLogTerm and log_index <= lastLogIndex:

                self.factory.set_persistant(term, candidateID)

                outmsg = (term, True)
                self.transport.write(str(outmsg))
                return

            # Otherwise refuse the vote
            outmsg = (self.factory.current_term, False)
            self.transport.write(str(outmsg))
            return


        elif data[0] == "AppendEntries":
            assert False
        else:
            assert False



class MeduseFactory(protocol.Factory):
    
    def __init__(self, name, reactor=reactor):
        self.name = name
        self.reactor = reactor

        ## Persistant state
        self.current_term = 0
        self.voted_for = None
        self.log = None

        # Attempt to open them from disk
        self.log = shelve.open('%s_log.db' % self.name, 'c')

        try:
            self.current_term = int(file("%s_term.txt" % self.name).read())
            self.voted_for = int(file("%s_voted.txt" % self.name).read())
        except:
            print "Cannot find persistant files"

        ## Ephemeral state
        self.state = FOLLOWER
        self.commit_index = len(self.log) - 1
        self.last_applied = None

        ## Leader State
        self.next_index = None
        self.matched_index = None

        ## Timers
        self.election_timeout = None
        self.reset_election_timeout()


    def get_last_log(self):
        if self.commit_index == -1:
            return (-1, -1, None)
        else:
            term, data = self.log[str(self.commit_index)]
            return (term, self.commit_index, data)


    def write_log(self, data):
        self.commit_index += 1
        self.log[str(self.commit_index)] = (self.current_term, data)
        self.log.sync()


    def reset_election_timeout(self):
        if self.election_timeout is not None:
            self.election_timeout.cancel()

        self.election_timeout = self.reactor.callLater(random.randint(150, 300) / 1000.0 , self.start_leader)


    def start_leader(self):
        print "Starting Election"
        
        self.election_timeout = None
        self.state = CANDIDATE
        self.set_persistant(self.current_term + 1, self.name)


    def set_persistant(self, term, vote):

        self.current_term = term
        self.voted_for = vote

        with file("%s_term.txt" % self.name, "w") as f0:
            f0.write(str(self.current_term))
            f0.flush()
        
        with  file("%s_voted.txt" % self.name, "w") as f1:
            f1.write(str(self.voted_for))
            f1.flush()


    def buildProtocol(self, addr):
        cli = MeduseProtocol(self)
        return cli

    def __del__(self):
        self.log.sync()
        self.log.close()

## Try some tests

from twisted.internet.task import Clock
import os.path
import os

from twisted.test import proto_helpers

def test_start():
    factory = MeduseFactory("node0")
    assert os.path.exists("node0_log.db")

    if os.path.exists("node0_voted.txt"):
        os.remove("node0_voted.txt")
    
    if os.path.exists("node0_term.txt"):
        os.remove("node0_term.txt")


def test_election_timeout():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)
    instance = factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    for _ in range(100):
        clock.advance(0.1)
        instance.dataReceived(("RequestVote", 0,0,0,0))
        

    assert factory.state == FOLLOWER
    clock.advance(1)
    assert factory.state == CANDIDATE
    
    assert os.path.exists("node0_voted.txt")
    os.remove("node0_voted.txt")
    
    assert os.path.exists("node0_term.txt")
    current_term = int(file("%s_term.txt" % "node0").read())
    assert current_term == 1
    os.remove("node0_term.txt")


def test_election_handling():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)
    factory.current_term = 100
    instance = factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    # Ensure old terms are rejected
    for _ in range(100):
        clock.advance(0.1)
        instance.dataReceived(("RequestVote", 50, 0, 0, 0))
        assert tr.value() == str((100, False))
        tr.clear()

    # Ensure newer terms cancel all states back to follower
    clock.advance(1)
    assert factory.state == CANDIDATE
    instance.dataReceived(("RequestVote", 150, 128, 0, 0))

    assert factory.state == FOLLOWER
    assert factory.voted_for == 128
    assert tr.value() == str((150, True))
