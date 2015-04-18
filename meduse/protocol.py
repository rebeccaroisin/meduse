from twisted.internet import protocol, reactor

import random
import shelve
import bsddb

FOLLOWER = 1
CANDIDATE = 2
LEADER = 3

## An instance of the raft server
class MeduseLeaderProtocol(protocol.Protocol):

    def __init__(self, factory):
        self.factory = factory
        self.buffer = ""

    def connectionMade(self):
        assert self.factory.state == CANDIDATE
        (log_term, log_index, _) = self.factory.get_last_log()
        msg = ("RequestVote", self.factory.current_term, self.factory.name, log_index, log_term)
        self.transport.write(str(msg))

    def dataReceived(self, data):
        data = eval(data)
        if data[0] == "ReplyVote":
            _, other_term, vote = data

            # If we are behind in terms become follower again
            print other_term, self.factory.current_term
            if other_term > self.factory.current_term:
                self.factory.back_to_follower()
                self.factory.set_persistant(other_term, None)
                return

            # Add a vote
            if vote:
                self.factory.votes += 1
                if self.factory.votes > len(self.factory.others) / 2:
                    self.factory.start_leader()



## Factories to connect to the other raft peers
class LeaderClientFactory(protocol.ClientFactory):

    def __init__(self, upper_factory):
        self.factory = upper_factory
        self.noisy = False


    def buildProtocol(self, addr):
        print 'Connected.'
        c = MeduseLeaderProtocol(self.factory)
        self.factory.conn += [c]
        return c


    def clientConnectionLost(self, connector, reason):
        print "Gone"
        if self.factory.state == LEADER:
            print 'Lost connection.  Reason:', reason, connector


## An instance of the raft server
class MeduseProtocol(protocol.Protocol):

    def __init__(self, factory):
        self.factory = factory
        self.buffer = ""

    def dataReceived(self, data):
        ## When we receive data by a candidate or leader back off our own timer
        self.factory.reset_election_timeout()

        data = eval(data)

        if data[0] == "RequestVote":
            # Parse incoming message
            _, term, candidateID, lastLogIndex, lastLogTerm = data

            # Go back to follower
            if term > self.factory.current_term:
                self.factory.back_to_follower()
                self.factory.set_persistant(term, None)


            # If old epoch, do not grant vote and send new epoch
            if term < self.factory.current_term:
                outmsg = ("ReplyVote", self.factory.current_term, False)
                self.transport.write(str(outmsg))
                return

            # If same epoch but we have already voted for someone
            (log_term, log_index, _) = self.factory.get_last_log()
            if self.factory.voted_for is None and \
                log_term <= lastLogTerm and log_index <= lastLogIndex:

                self.factory.set_persistant(term, candidateID)

                outmsg = ("ReplyVote", term, True)
                self.transport.write(str(outmsg))
                return

            # Otherwise refuse the vote
            outmsg = ("ReplyVote", self.factory.current_term, False)
            self.transport.write(str(outmsg))
            return


        elif data[0] == "AppendEntries":
            pass

        else:
            assert False



class MeduseFactory(protocol.Factory):
    
    def __init__(self, name, reactor=reactor):
        self.others = []
        self.conn = []
        self.votes = None

        self.LeaderFactory = LeaderClientFactory(self)

        self.name = name
        self.reactor = reactor

        ## Persistant state
        self.current_term = 0
        self.voted_for = None
        self.log = None

        # Attempt to open them from disk
        db = bsddb.btopen('%s_log.db' % self.name, 'c')
        self.log = shelve.BsdDbShelf(db)
        #self.log = shelve.open('%s_log.db' % self.name, 'c')

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
        self.match_index = None

        ## Timers
        self.heartbeat_timeout = None
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
        if self.state == FOLLOWER or self.state == CANDIDATE:
            if self.election_timeout is not None and self.election_timeout.active():
                self.election_timeout.cancel()

            self.election_timeout = self.reactor.callLater(random.randint(150, 300) / 1000.0 , self.start_leader_election)


    def back_to_follower(self):
        print "Back to follower"
        self.state = FOLLOWER
        self.votes = None

        ## Cancel all connections
        while len(self.conn) > 0:
            c = self.conn.pop()
            c.transport.loseConnection()

        ## Reset election timeout
        self.reset_election_timeout()


    def start_leader_election(self):
        print "Starting Election"
        
        ## In case the election takes too long, we move forward
        #  that happens in case of split votes.
        if self.election_timeout is not None and self.election_timeout.active():
            self.election_timeout.cancel()

        self.election_timeout = None
        self.reset_election_timeout()

        ## 
        self.state = CANDIDATE
        self.set_persistant(self.current_term + 1, self.name)
        self.votes = 1

        # self.conn = []
        for (host, port, name) in self.others:
            reactor.connectTCP(host, port, self.LeaderFactory)


    def reset_heartbeat(self):
        if self.state == LEADER:
            if self.heartbeat_timeout is not None and self.heartbeat_timeout.active():
                self.heartbeat_timeout.cancel()

            print "Reset heartbeat"
            self.heartbeat_timeout = self.reactor.callLater(25 / 1000.0 , self.send_heartbeat)
        else:
            raise Exception("I am not the leader")


    def send_heartbeat(self):
        #print "hb"
        self.heartbeat_timeout = None

        (log_term, log_index, _) = self.get_last_log()
        msg = ("AppendEntries", self.current_term, self.name, log_index, log_term, [], log_index)

        for c in self.conn:            
            c.transport.write(str(msg))

        self.reset_heartbeat()

    def start_leader(self):
        print "I am legend."
        # Cancel the time-out, we made it to leader
        if self.election_timeout is not None:
            self.election_timeout.cancel()
        self.election_timeout = None

        ## Set the heartbeats
        self.state = LEADER
        self.reset_heartbeat()

        # Initialize your view of the others
        (log_term, log_index, _) = self.get_last_log()
        self.next_index = [log_index] * len(self.others)
        self.match_index = [-1] * len(self.others)

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
        instance.dataReceived(str(("RequestVote", 0,0,0,0)))
        

    assert factory.state == FOLLOWER
    clock.advance(1)
    assert factory.state == CANDIDATE
    
    assert os.path.exists("node0_voted.txt")
    os.remove("node0_voted.txt")
    
    assert os.path.exists("node0_term.txt")
    current_term = int(file("%s_term.txt" % "node0").read())
    assert current_term == 1
    os.remove("node0_term.txt")


import mock

@mock.patch('twisted.internet.reactor.connectTCP')
def test_election_handling(mock_reactor):
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)
    factory.current_term = 100
    instance = factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    # Ensure old terms are rejected
    for _ in range(100):
        clock.advance(0.1)
        instance.dataReceived(str(("RequestVote", 50, 0, 0, 0)))
        assert tr.value() == str(("ReplyVote", 100, False))
        tr.clear()

    # Define some others
    factory.others = [(1,1, "Node1"), (2,2, "Node2")]

    # Ensure newer terms cancel all states back to follower
    clock.advance(1)
    assert len(mock_reactor.call_args_list) == 2

    assert factory.state == CANDIDATE
    instance.dataReceived(str(("RequestVote", 150, 128, 0, 0)))

    assert factory.state == FOLLOWER
    assert factory.voted_for == 128
    assert tr.value() == str(("ReplyVote", 150, True))

    if os.path.exists("node0_voted.txt"):
        os.remove("node0_voted.txt")
    
    if os.path.exists("node0_term.txt"):
        os.remove("node0_term.txt")


def test_leader_client():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Put in candidate state
    factory.start_leader_election()
    client_factory = LeaderClientFactory(factory)

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    instance.dataReceived(str(("ReplyVote", 0, True)))
    print "Votes", factory.votes
    assert factory.state == LEADER

    if os.path.exists("node0_voted.txt"):
        os.remove("node0_voted.txt")
    
    if os.path.exists("node0_term.txt"):
        os.remove("node0_term.txt")

def test_leader_backoff():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Puts the protocol in CANDIDATE STATE
    factory.start_leader_election()
    client_factory = factory.LeaderFactory

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    # print tr.value()
    instance.dataReceived(str(("ReplyVote", 10, False)))
    print "Votes", factory.votes
    assert factory.state == FOLLOWER

    if os.path.exists("node0_voted.txt"):
        os.remove("node0_voted.txt")
    
    if os.path.exists("node0_term.txt"):
        os.remove("node0_term.txt")

def test_leader_heartbeat():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Put in candidate state
    factory.start_leader_election()
    client_factory = LeaderClientFactory(factory)

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    instance.dataReceived(str(("ReplyVote", 0, True)))
    print "Votes", factory.votes
    assert factory.state == LEADER

    print factory.conn
    assert len(factory.conn) == 1
    print factory.election_timeout, factory.match_index, factory.next_index

    tr.clear()
    clock.pump([0.01] * 100)
    print tr.value()
    assert factory.state == LEADER

    if os.path.exists("node0_voted.txt"):
        os.remove("node0_voted.txt")
    
    if os.path.exists("node0_term.txt"):
        os.remove("node0_term.txt")
