import socket
from sock import Sock
import abc
import threading
import cPickle as pickle
import SQL_ORM
import sqlite3


class TCP(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, (ip, port)):
        self.ip = ip
        self.port = port


class Server(TCP):
    """An object that makes it easier to use the protocol to accept and communicate with clients."""

    ready = "READY"

    def __init__(self, (ip, port)):
        """(ip, port) is the ip and port combination you would pass to socket.bind.
        db_name is the name of the DB excluding file name extension (.db assumed)."""
        super(Server, self).__init__((ip, port))
        self._keep_listening = False

    def listen(self, handler=lambda sock: None, backlog=5, verify_join=True, verbose=True, **kwargs):
        """Listen for new client trying to connect and accept them.
        The handler argument is a callable that the first argument it takes is the socket of the accepted clients.
        Other arguments can be passed through kwargs.
        The default value of handler does nothing.
        The backlog argument specifies the maximum number of queued connections and should be at least 0;
        the maximum value is system-dependent (usually 5), the minimum value is forced to 0.
        The verify_join argument specifies whether the function should
        join all client threads once the server is closed or not."""
        self._keep_listening = True
        client_threads = []
        sock = Sock()
        sock.bind((self.ip, self.port))
        sock.listen(backlog)
        while self._keep_listening:
            client_sock, client_addr = sock.accept()
            Server.connect(client_sock)
            _printif(verbose, "Connected to client @ %s" % client_addr[0])
            client = threading.Thread(target=handler, args=(client_sock, ), kwargs=kwargs)
            client_threads.append(client)
            client.start()
        if verify_join:
            Server.join(timeout=None, *client_threads)
        self._keep_listening = False

    def stop_listening(self):
        """Stop listening for and accepting new clients."""
        self._keep_listening = False

    @staticmethod
    def connect(sock):
        """Typically at the start of the communication
        to make sure both sides are synchronized and are correctly connected.
        Can be called later to make both sides are still synchronized."""
        try:
            sock.send_by_size(Server.ready)
        except socket.error:
            raise socket.error("Connection to client @ %s failed." % sock.getpeername()[0])

    @staticmethod
    def join(timeout=None, *threads):
        """Join all the threads given, with the given timeout."""
        for thread in threads:
            thread.join(timeout)


class Client(TCP):
    """An object that makes it easier to use the protocol to communicate with the server."""
    def __init__(self, (ip, port)):
        """(ip, port) is the ip and port combination you would pass to socket.bind.."""
        self.sock = Sock()
        super(Client, self).__init__((ip, port))

    def connect(self):
        """Connect to the server."""
        try:
            self.sock.connect((self.ip, self.port))
            if self.sock.recv_by_size() != Server.ready:
                raise socket.error
        except socket.error:
            raise socket.error("Connection to server @ %s failed." % self.sock.getpeername()[0])


def _printif(i, *s):
    """print s if i."""
    if i:
        print("".join(str(word) for word in s))


class SQLServer(Server):
    """An object that can handle running an sql server."""

    success = "SUCCESS"
    failure = "FAILURE"

    def __init__(self, (ip, port), db_name="ORM"):
        """(ip, port) is the ip and port combination you would pass to socket.bind.
        db_name is the name of the DB excluding file name extension (.db assumed)."""
        super(SQLServer, self).__init__((ip, port))
        self.orm = SQL_ORM.ORM(db_name)

    def listen(self, handler=None, backlog=5, verify_join=True, verbose=True, **kwargs):
        """Listen for new client trying to connect and accept them.
        If handler is omitted or None, self.handle_client is used and kwargs is ignored,
        otherwise it is like Server.listen.
        The backlog argument specifies the maximum number of queued connections and should be at least 0;
        the maximum value is system-dependent (usually 5), the minimum value is forced to 0.
        The verify_join argument specifies whether the function should
        join all client threads once the server is closed or not."""
        if handler is None:
            super(SQLServer, self).listen(self.handle_client, backlog=backlog, verify_join=verify_join, verbose=verbose,
                                          announce=verbose)
        else:
            super(SQLServer, self).listen(handler, backlog=backlog, verify_join=verify_join, verbose=verbose, **kwargs)

    def handle_client(self, sock, announce=True):
        """The function that Server.listen calls with new clients."""
        while True:
            try:
                sock.settimeout(None)
                self.get_request(sock)
            except socket.error:
                break
        _printif(announce, "Client @ %s disconnected" % sock.getpeername()[0])
        sock.close()

    def get_request(self, sock):
        """Receives a request from the client, and calls the function that can answer it."""
        request = ""
        try:
            request = sock.recv_by_size()
            if request == SQLClient.get:
                self.send(sock)
            elif request == SQLClient.add_str:
                self.add(sock)
            elif request == SQLClient.update_str:
                self.update(sock)
            elif request == SQLClient.delete_str:
                self.delete(sock)
            else:
                raise socket.error
        except socket.error:
            if request == "":
                raise socket.error("Client @ %s disconnected" % sock.getpeername()[0])
            raise socket.error("Communication failed with client @ %s." % sock.getpeername()[0])

    def send(self, sock):
        """Uses the parameters received from the client and SQL_ORM to send information to the client."""
        received = sock.recv_by_size().split("~")
        table = received[0][0].upper() + received[0][1:].lower()
        try:
            ratio = received[1]
            constraints = pickle.loads(received[2])
        except IndexError:
            ratio = None
            constraints = None
        if table == "Players":
            sock.send_by_size(pickle.dumps(self._handle_player_sends(ratio, constraints),
                                           pickle.HIGHEST_PROTOCOL))
        elif table == "Teams":
            sock.send_by_size(pickle.dumps(self._handle_team_sends(ratio, constraints),
                                           pickle.HIGHEST_PROTOCOL))
        else:
            sock.send_by_size(pickle.dumps("ERROR~UNKNOWN TABLE~003~'%s'" % table, pickle.HIGHEST_PROTOCOL))

    def _handle_player_sends(self, ratio=None, constraints=None):
        """Returns the information for send requests on the Players table.
        The return value is a list with sql.Player object, unless an error occurred, than a string is returned."""
        try:
            if None in (ratio, constraints):
                return self.orm.player.get_players()
            try:
                if isinstance(constraints["team_id"], basestring):
                    constraints["team_id"] = self.orm.team.get_teams(name=constraints["team_id"])[0].id
            except KeyError:
                pass    # teams_id not in list.
            for key in constraints:
                if not isinstance(constraints[key], basestring):
                    return self.orm.player.get_players(ratio, **constraints)    # Cannot use contains, not string.
            return self.orm.player.get_players_contains(**constraints)
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"

    def _handle_team_sends(self, ratio=None, constraints=None):
        """Returns the information for send requests on the Teams table.
        The return value is a list with sql.Team object, unless an error occurred, than a string is returned."""
        try:
            if None in (ratio, constraints):
                return self.orm.team.get_teams()
            for key in constraints:
                if not isinstance(constraints[key], basestring):
                    return self.orm.team.get_teams(ratio, **constraints)    # Cannot use contains, not string.
            return self.orm.team.get_teams_contains(**constraints)
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"

    def add(self, sock):
        """Uses parameter received from the client to add a row to the DB."""
        received = sock.recv_by_size().split("~")
        try:
            table = received[0][0].upper() + received[0][1:].lower()
            values = pickle.loads(received[1])
            values["id"] = None
        except (IndexError, KeyError):
            sock.send_by_size(pickle.dumps("ERROR~INCOMPLETE REQUEST~004~None", pickle.HIGHEST_PROTOCOL))
            return
        if table == "Players":
            sock.send_by_size(pickle.dumps(self._handle_player_adds(values), pickle.HIGHEST_PROTOCOL))
        elif table == "Teams":
            sock.send_by_size(pickle.dumps(self._handle_team_adds(values), pickle.HIGHEST_PROTOCOL))
        else:
            sock.send_by_size(pickle.dumps("ERROR~UNKNOWN TABLE~003~'{}'".format(table), pickle.HIGHEST_PROTOCOL))

    def _handle_player_adds(self, values):
        """Try to add the player to the DB based on information from the client."""
        try:
            if isinstance(values["team_id"], basestring):
                values["team_id"] = self.orm.team.get_teams(name=values["team_id"])[0].id
            if self.orm.player.add_player(SQL_ORM.PlayerORM.dict_to_object(values)):
                return SQLServer.success
            return SQLServer.failure
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"
        except KeyError:
            return "ERROR~INCOMPLETE DICT~004~None"
        except IndexError:
            return "ERROR~TEAM NOT RECOGNIZED~005~{}".format(str(values["team_id"]))

    def _handle_team_adds(self, values):
        """Try to add the team to the DB based on information from the client."""
        try:
            if self.orm.team.add_team(SQL_ORM.TeamORM.dict_to_object(values)):
                return SQLServer.success
            return SQLServer.failure
        except (sqlite3.Error, KeyError):
            return "ERROR~UNKNOWN~000~None"

    def update(self, sock):
        """Uses parameter received from the client to update a row in the DB."""
        received = sock.recv_by_size().split("~")
        try:
            table = received[0][0].upper() + received[0][1:].lower()
            obj_id = int(received[1])
            updates = pickle.loads(received[2])
        except IndexError:
            sock.send_by_size(pickle.dumps("ERROR~INCOMPLETE REQUEST~001~None", pickle.HIGHEST_PROTOCOL))
            return
        except TypeError:
            sock.send_by_size(pickle.dumps("ERROR~WRONG ARGUMENT~002~{}".format(received[1]), pickle.HIGHEST_PROTOCOL))
            return
        if table == "Players":
            sock.send_by_size(pickle.dumps(self._handle_player_updates(obj_id, **updates), pickle.HIGHEST_PROTOCOL))
        elif table == "Teams":
            sock.send_by_size(pickle.dumps(self._handle_team_updates(obj_id, **updates), pickle.HIGHEST_PROTOCOL))
        else:
            sock.send_by_size(pickle.dumps("ERROR~UNKNOWN TABLE~003~'{}'".format(table), pickle.HIGHEST_PROTOCOL))

    def _handle_player_updates(self, player_id, **updates):
        """Try to update the player on the DB based on information from the client."""
        try:
            if self.orm.player.update_player(player_id, **updates):
                return SQLServer.success
            return SQLServer.failure
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"

    def _handle_team_updates(self, team_id, **updates):
        """Try to update the team on the DB based on information from the client."""
        try:
            if self.orm.team.update_team(team_id, **updates):
                return SQLServer.success
            return SQLServer.failure
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"

    def delete(self, sock):
        """Uses parameter received from the client to delete rows from the DB."""
        received = sock.recv_by_size().split("~")
        try:
            table = received[0][0].upper() + received[0][1:].lower()
            obj_id = int(received[1])
        except (IndexError, KeyError):
            sock.send_by_size(pickle.dumps("ERROR~INCOMPLETE REQUEST~004~None", pickle.HIGHEST_PROTOCOL))
            return
        if table == "Players":
            sock.send_by_size(pickle.dumps(self._handle_player_deletes(obj_id), pickle.HIGHEST_PROTOCOL))
        elif table == "Teams":
            sock.send_by_size(pickle.dumps(self._handle_team_deletes(obj_id), pickle.HIGHEST_PROTOCOL))
        else:
            sock.send_by_size(pickle.dumps("ERROR~UNKNOWN TABLE~003~'{}'".format(table), pickle.HIGHEST_PROTOCOL))

    def _handle_player_deletes(self, player_id):
        """Try to delete the player on the DB based on information from the client."""
        try:
            if self.orm.player.delete_player(player_id):
                return SQLServer.success
            return SQLServer.failure
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"

    def _handle_team_deletes(self, team_id):
        """Try to delete the team on the DB based on information from the client."""
        try:
            if self.orm.team.delete_team(team_id):
                return SQLServer.success
            return SQLServer.failure
        except sqlite3.Error:
            return "ERROR~UNKNOWN~000~None"


class SQLClient(Client):
    """A class that makes it easy to communicate with the SQLServer."""

    get = "GET"
    equal = "="
    bigger = ">"
    smaller = "<"

    add_str = "ADD"
    update_str = "UPDATE"
    delete_str = "DELETE"

    def __init__(self, (ip, port)):
        """Create a new SQLClient object.
        (ip, port) is the ip and port combination you would pass to socket.bind."""
        super(SQLClient, self).__init__((ip, port))

    def receive(self, table, ratio="=", **constraints):
        """Receive information from the server. table is the type of information.
        ratio - Should it be Client.equal\Client.bigger\Client.smaller than the provided value.
        constraints is like on SQL_ORM.
        Returns the information received back from the server.
        In case server sent back an error, ValueError is raised with information from the server as description."""
        self._send_receive_request(table, ratio, **constraints)
        return self._receive_receive_request()

    def _send_receive_request(self, table, ratio="=", **constraints):
        """Constructs the message to be sent for a receive request to the server and sends it."""
        try:
            self.sock.send_by_size(SQLClient.get)
            if not constraints:
                self.sock.send_by_size(str(table)[0].upper() + str(table)[1:].lower())
            else:
                self.sock.send_by_size(str(table)[0].upper() + str(table)[1:].lower() + "~" + str(ratio) + "~"
                                       + pickle.dumps(constraints, pickle.HIGHEST_PROTOCOL))
        except socket.error:
            raise socket.error("Could not send request to the server.")
        except pickle.PicklingError:
            raise pickle.PicklingError("Could not pickle values.")

    def _receive_receive_request(self):
        """Receives the answer to a receive request from the server."""
        try:
            info = pickle.loads(self.sock.recv_by_size())
            if isinstance(info, basestring):
                err = info.split("~")
                raise ValueError("ERROR %s. Information: %s" % (err[1], err[3]))
        except socket.error:
            raise socket.error("Could not receive information from the server.")
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError("Server could not send information.")
        return info

    def add(self, table, **values):
        """Add a row to the table on the server's DB.
        Returns True for success, False for failure to add to DB."""
        self._send_add_request(table, **values)
        return self._server_execution_success()

    def _send_add_request(self, table, **values):
        """Constructs the message to be sent for an add request to the server and sends it."""
        try:
            self.sock.send_by_size(SQLClient.add_str)
            self.sock.send_by_size(table + "~" + pickle.dumps(values, pickle.HIGHEST_PROTOCOL))
        except socket.error:
            raise socket.error("Could not send request to the server.")
        except pickle.PicklingError:
            raise pickle.PicklingError("Could not pickle values.")

    def update(self, table, obj_id, **updates):
        """Update information on the server.
        Returns True for success, False for failure to add to DB."""
        self._send_update_request(table, obj_id, **updates)
        return self._server_execution_success()

    def _send_update_request(self, table, obj_id, **updates):
        """Constructs the message to be sent to update DB on the server."""
        try:
            self.sock.send_by_size(SQLClient.update_str)
            self.sock.send_by_size(table + "~" + str(obj_id) + "~" + pickle.dumps(updates, pickle.HIGHEST_PROTOCOL))
        except socket.error:
            raise socket.error("Could not send request to the server.")
        except pickle.PicklingError:
            raise pickle.PicklingError("Could not pickle values.")

    def delete(self, table, obj_id):
        """Delete information from the server's DB.
        Returns True for success, False for failure to delete from DB."""
        self._send_delete_request(table, obj_id)
        return self._server_execution_success()

    def _send_delete_request(self, table, obj_id):
        """Constructs the message to be sent to delete from the DB on the server."""
        try:
            self.sock.send_by_size(SQLClient.delete_str)
            self.sock.send_by_size(table + "~" + str(obj_id))
        except socket.error:
            raise socket.error("Could not send request to the server.")
        except pickle.PicklingError:
            raise pickle.PicklingError("Could not pickle values.")

    def _server_execution_success(self):
        """Receives the server's response and
        returns whether the server successfully executed a non-receive request based on response."""
        try:
            response = pickle.loads(self.sock.recv_by_size())
        except socket.error:
            raise socket.error("Could not receive answer from the server.")
        except pickle.UnpicklingError:
            raise pickle.UnpicklingError("Server could not send information")
        if response == SQLServer.success:
            return True
        if response == SQLServer.failure:
            return False
        if response.startswith("ERROR"):
            err = response.split("~")
            raise socket.error("ERROR %s. Information: %s" % (err[1], err[3]))
        else:
            raise socket.error("Could not receive information from the server.")
