from twisted.internet import protocol, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

import random
import shelve
import bsddb
import struct
import json


FOLLOWER = 1
CANDIDATE = 2
LEADER = 3


def package_data(data):
    message = json.dumps(data)
    header = struct.pack("I", (len(message)))
    return header + message

def unpackage_data(data):
    if len(data) < 4:
        return None, data

    length = struct.unpack("I", data[:4])[0]
    if len(data) >= length + 4:
        message = data[4:4+length]
        data = data[4+length:]
        message = json.loads(message)
        return message, data
    else:
        return None, data


## An instance of the raft server
class MeduseLeaderProtocol(protocol.Protocol):

    def __init__(self, factory):
        self.leader_factory = factory
        self.factory = factory.factory
        self.buffer = ""


    def dataReceived(self, data):
        self.buffer += data
        while True:
            message, self.buffer = unpackage_data(self.buffer)
            if message is not None:
                self.process_message(message)
            else:
                break


    def connectionMade(self):
        self.leader_factory.retry = 0.2
        self.leader_factory.proto = self

        if self.factory.state == CANDIDATE:
            (log_term, log_index, _) = self.factory.get_last_log()
            msg = ("RequestVote", self.factory.current_term, self.factory.name, log_index, log_term)
            self.transport.write(package_data(msg))


    def process_message(self, data):
        # data = eval(data) # BAD BAD BAD

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

        elif data[0] == "ReplyAppendEntries":
            _, other_term, success = data

            print other_term, self.factory.current_term
            if other_term > self.factory.current_term:
                self.factory.back_to_follower()
                self.factory.set_persistant(other_term, None)
                return

            ## TODO



## Factories to connect to the other raft peers
class LeaderClientFactory(protocol.ClientFactory):

    def __init__(self, upper_factory, client_info):
        self.factory = upper_factory
        self.client_info = client_info

        self.noisy = False
        self.proto = None
        self.retry = 0.2
        self.reconnect_timer = None

    def schedule_reconnect(self):
        ## Do not reschedule
        if self.reconnect_timer is not None and self.reconnect_timer.active():
            return

        assert self.proto == None
        self.retry *= (1.5 + random.random()) 
        self.retry = min(self.retry, 5)
        self.reconnect_timer = self.factory.reactor.callLater(self.retry , self.reconnect)

        
    def reconnect(self):
        assert self.proto == None
        host, port, name = self.client_info
        
        reactor.connectTCP(host, port, self)


    def buildProtocol(self, addr):
        print 'Connected to %s' % self.client_info[2]

        assert self.proto is None
        self.factory.conn += [ self ]
        proto = MeduseLeaderProtocol(self)
        return proto


    def clientConnectionLost(self, connector, reason):
        print "Connection Lost to %s" % self.client_info[2]

        assert self.proto is not None
        self.proto = None
        if self.factory.state in [LEADER, CANDIDATE]:
            self.schedule_reconnect()


    def clientConnectionFailed(self, connector, reason):
        print "Connection Failed %s" % self.client_info[2]

        assert self.proto is not None
        self.proto = None
        if self.factory.state in [LEADER, CANDIDATE]:
            self.schedule_reconnect()

## An instance of the raft server
class MeduseProtocol(protocol.Protocol):

    def __init__(self, factory):
        self.factory = factory
        self.buffer = ""


    def dataReceived(self, data):
        self.buffer += data
        while True:
            message, self.buffer = unpackage_data(self.buffer)
            if message is not None:
                self.process_message(message)
            else:
                break


    def process_message(self, data):
        ## When we receive data by a candidate or leader back off our own timer
        self.factory.reset_election_timeout()

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
                self.transport.write(package_data(outmsg))
                return

            # If same epoch but we have already voted for someone
            (log_term, log_index, _) = self.factory.get_last_log()
            if self.factory.voted_for is None and \
                log_term <= lastLogTerm and log_index <= lastLogIndex:

                self.factory.set_persistant(term, candidateID)

                outmsg = ("ReplyVote", term, True)
                self.transport.write(package_data(outmsg))
                return

            # Otherwise refuse the vote
            outmsg = ("ReplyVote", self.factory.current_term, False)
            self.transport.write(package_data(outmsg))
            return


        elif data[0] == "AppendEntries":
            _, term, leader_name, log_index, log_term, entries, leader_commit = data

            print "logindex", log_index
            if str(log_index) in self.factory.log:
                (our_log_term, our_log_data) = self.factory.log[str(log_index)]

            LogOK = (log_index == 0)
            LogOK |= (log_index > 0) and \
                        (log_index <= len(self.factory.log)) and \
                        (log_term == our_log_term)
            
            if not LogOK:
                print "LOGOK", LogOK
                print "Part1", log_index, (0 < log_index <= len(self.factory.log))
                print (log_term == our_log_term)


            # Go back to follower
            if term > self.factory.current_term:
                self.factory.back_to_follower()
                self.factory.set_persistant(term, None)
            

            # If term of leader is smaller return our current term
            reject_1 = term < self.factory.current_term
            reject_2 = (term == self.factory.current_term) and \
                       (self.factory.state == FOLLOWER) and \
                       (not LogOK)

            # Reasons to reject
            if reject_1 or reject_2:
                outmsg = ("ReplyAppendEntries", self.factory.current_term, False)
                self.transport.write(package_data(outmsg))
                return


            # Say nothing? (Strange)
            if term == self.factory.current_term and \
               self.factory.state == CANDIDATE:
                self.factory.back_to_follower()
                # return

            # Delete entries
            rindex = log_index + 1
            if len(entries) > 0 and len(self.factory.log) >= rindex:
                if str(rindex) in self.factory.log:
                    (r_term, r_data) = self.factory.log[str(rindex)]
                    if r_term != entries[0][0]:
                        print "Remove shit"
                        while str(rindex) in self.factory.log:
                            del self.factory.log[str(rindex)]
                            rindex += 1
                        print self.factory.log


            # Append all entries
            if len(entries) > 0 and len(self.factory.log) == log_index:
                print "Add shit"
                v = log_index + 1
                for e in entries:
                    self.factory.log[str(v)] = (e[0], e[1])
                    v += 1
                print self.factory.log


            # Accept request 
            if (term == self.factory.current_term) and \
                    (self.factory.state == FOLLOWER) and \
                    LogOK:
                
                rindex = log_index + 1
                if len(entries) == 0:
                    self.factory.commit_index = leader_commit
                    outmsg = ("ReplyAppendEntries", self.factory.current_term, True, self.factory.commit_index)
                    self.transport.write(package_data(outmsg))
                    # return

                print "idx",  len(self.factory.log), rindex
                if len(entries) > 0 and len(self.factory.log) >= rindex:
                    (r_term, _) = self.factory.log[str(rindex)]

                    (entry_term, _) = entries[0]

                    print "The term", entry_term, r_term
                    if (entry_term == r_term):
                        self.factory.commit_index = leader_commit
                        outmsg = ("ReplyAppendEntries", self.factory.current_term, True, self.factory.commit_index + len(entries))
                        self.transport.write(package_data(outmsg))
                        ## TODO append entries
                        # return


        else:
            assert False



class MeduseFactory(protocol.Factory):
    
    def debug_cleanup(self):
        if os.path.exists("%s_log.db.txt" % self.name):
            os.remove("%s_log.db.txt" % self.name)
    
        if os.path.exists("%s_term.txt" % self.name):
            os.remove("%s_term.txt" % self.name)

        if os.path.exists("%s_voted.txt" % self.name):
            os.remove("%s_voted.txt" % self.name)
    

    def __init__(self, name, reactor=reactor):
        self.others = []
        self.conn = []
        self.votes = None

        # self.LeaderFactory = LeaderClientFactory(self)

        self.name = name
        self.reactor = reactor

        ## Persistant state
        self.current_term = 0
        self.voted_for = None
        self.log = None

        # Attempt to open them from disk
        db = bsddb.btopen('%s_log.db' % self.name, 'c')
        self.log = shelve.BsdDbShelf(db)
        if len(self.log) == 0:
            self.log[str(1)] = (0, "START")
            self.log.sync()

        try:
            self.current_term = int(file("%s_term.txt" % self.name).read())
            self.voted_for = int(file("%s_voted.txt" % self.name).read())
        except:
            print "Cannot find persistant files"

        ## Ephemeral state
        self.state = FOLLOWER
        self.commit_index = 1
        self.last_applied = None

        ## Leader State
        self.next_index = None
        self.match_index = None

        ## Timers
        self.heartbeat_timeout = None
        self.election_timeout = None
        self.reset_election_timeout(delta=5)


    def get_last_log(self):
        if self.commit_index == -1:
            return (-1, -1, None)
        else:
            term, data = self.log[str(len(self.log))]
            return (term, self.commit_index, data)


    def write_log(self, data):
        self.commit_index += 1
        self.log[str(self.commit_index)] = (self.current_term, data)
        self.log.sync()


    def reset_election_timeout(self, delta = 0):
        if self.state == FOLLOWER or self.state == CANDIDATE:
            if self.election_timeout is not None and self.election_timeout.active():
                self.election_timeout.cancel()

            self.election_timeout = self.reactor.callLater(delta + random.randint(150, 300) / 1000.0 , self.start_leader_election)


    def back_to_follower(self):
        print "Back to follower"
        self.state = FOLLOWER
        self.votes = None

        ## Cancel all connections
        while len(self.conn) > 0:
            c = self.conn.pop()
            if c.proto is not None:
                c.proto.transport.loseConnection()

        ## Reset election timeout
        if self.heartbeat_timeout is not None and self.heartbeat_timeout.active():
            self.heartbeat_timeout.cancel()
            self.heartbeat_timeout = None

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

            # Make a specific factory for this peer
            client_info = (host, port, name)
            c = LeaderClientFactory(self, client_info)

            reactor.connectTCP(host, port, c)


    def reset_heartbeat(self):
        if self.state == LEADER:
            if self.heartbeat_timeout is not None and self.heartbeat_timeout.active():
                self.heartbeat_timeout.cancel()

            self.heartbeat_timeout = self.reactor.callLater(25 / 1000.0 , self.send_heartbeat)
        else:
            raise Exception("I am not the leader")


    def send_heartbeat(self):
        #print "hb"
        self.heartbeat_timeout = None

        (log_term, log_index, _) = self.get_last_log()
        msg = ("AppendEntries", self.current_term, self.name, log_index, log_term, [], log_index)

        for c in self.conn:  
            if c.proto is not None:
                c.proto.transport.write(package_data(msg))

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
        instance.dataReceived(package_data(("RequestVote", 0,0,0,0)))
        

    assert factory.state == FOLLOWER
    clock.advance(1)
    assert factory.state == CANDIDATE
    
    assert os.path.exists("node0_voted.txt")
    os.remove("node0_voted.txt")
    
    assert os.path.exists("node0_term.txt")
    current_term = int(file("%s_term.txt" % "node0").read())
    assert current_term == 1
    os.remove("node0_term.txt")

    factory.debug_cleanup()

import mock

@mock.patch('twisted.internet.reactor.connectTCP')
def test_election_handling(mock_reactor):
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)
    factory.current_term = 100
    factory.others = [("127.0.0.1", 8080, "None1")]
    instance = factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    # Ensure old terms are rejected
    for _ in range(100):
        clock.advance(0.1)
        instance.dataReceived(package_data(("RequestVote", 50, 0, 0, 0)))
        assert tr.value() == package_data(("ReplyVote", 100, False))
        tr.clear()

    # Define some others
    factory.others = [(1,1, "Node1"), (2,2, "Node2")]

    # Ensure newer terms cancel all states back to follower
    clock.advance(1)
    assert len(mock_reactor.call_args_list) == 2

    assert factory.state == CANDIDATE
    instance.dataReceived(package_data(("RequestVote", 150, 128, 1, 0)))

    assert factory.state == FOLLOWER
    assert factory.voted_for == 128
    assert tr.value() == package_data(("ReplyVote", 150, True))

    factory.debug_cleanup()

def test_leader_client():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Put in candidate state
    factory.start_leader_election()
    client_factory = LeaderClientFactory(factory, ("127.0.0.1", 8080, "foo1"))

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    instance.dataReceived(package_data(("ReplyVote", 0, True)))
    print "Votes", factory.votes
    assert factory.state == LEADER

    factory.debug_cleanup()


def test_leader_backoff():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Puts the protocol in CANDIDATE STATE
    factory.start_leader_election()
    client_factory = LeaderClientFactory(factory, ("127.0.0.1", 8080, "foo1"))

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    # print tr.value()
    instance.dataReceived(package_data(("ReplyVote", 10, False)))
    print "Votes", factory.votes
    assert factory.state == FOLLOWER

    factory.debug_cleanup() 


def test_leader_heartbeat():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)

    ## Put in candidate state
    factory.start_leader_election()
    client_factory = LeaderClientFactory(factory, ("127.0.0.1", 8080, "foo1"))

    instance = client_factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    assert factory.state == CANDIDATE

    instance.dataReceived(package_data(("ReplyVote", 0, True)))
    print "Votes", factory.votes
    assert factory.state == LEADER

    print factory.conn
    assert len(factory.conn) == 1
    print factory.election_timeout, factory.match_index, factory.next_index

    tr.clear()
    clock.pump([0.005] * 200)

    import re
    assert len(re.findall("AppendEntries", tr.value())) > 5

    assert factory.state == LEADER
    

    factory.debug_cleanup()


def test_package():
    d = [1,2,4,5, "Hello"]
    p = package_data(d)
    d0, _ = unpackage_data(p)
    assert d == d0

    d = [1,2,4,5, "Hello"] * 100
    p = package_data(d)
    p = p[:30]
    d0, p0 = unpackage_data(p)
    assert d0 == None
    assert p0 == p


# @mock.patch('twisted.internet.reactor.connectTCP')
def test_append_handling():
    clock = Clock()
    
    factory = MeduseFactory("node0", reactor=clock)
    factory.current_term = 100
    factory.others = [("127.0.0.1", 8080, "None1")]

    instance = factory.buildProtocol(None)
    tr = proto_helpers.StringTransport()
    instance.makeConnection(tr)

    msg = ("AppendEntries", 100, "AttillaTheHun", 0, 0, [], 0)
    instance.dataReceived(package_data(msg))
    assert unpackage_data(tr.value())[0][2] == True

    tr.clear()
    assert factory.current_term == 100
    msg = ("AppendEntries", 101, "AttillaTheHun", 0, 0, [], 0)
    instance.dataReceived(package_data(msg))
    assert unpackage_data(tr.value())[0][2] == True
    assert factory.current_term == 101

    tr.clear()
    msg = ("AppendEntries", 90, "AttillaTheHun", 0, 0, [], 0)
    instance.dataReceived(package_data(msg))
    assert unpackage_data(tr.value())[0][2] == False

    ## Test appending on good log
    tr.clear()
    assert len(factory.log) == 1
    msg = ("AppendEntries", 101, "AttillaTheHun", 1, 0, [(101, "Pony")], 0)
    instance.dataReceived(package_data(msg))
    assert unpackage_data(tr.value())[0][2] == True

    ## Test appending after deleting bad log
    tr.clear()
    assert len(factory.log) == 2
    msg = ("AppendEntries", 102, "AttillaTheHun", 1, 0, [(102, "Pony")], 0)
    instance.dataReceived(package_data(msg))
    assert unpackage_data(tr.value())[0][2] == True

    print unpackage_data(tr.value())
    
    factory.debug_cleanup()    