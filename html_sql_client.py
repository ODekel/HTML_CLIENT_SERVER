__author__ = 'Omer'

from protocol import SQLClient
import socket
import sys
import webbrowser
import os


PORT = 53326

HTML_PLAYERS_PATH = "HTML/players.html"
HTML_TEAMS_PATH = "HTML/teams.html"
HTML_PLAYERS_NEW_PATH = "HTML/new_players.html"
HTML_TEAMS_NEW_PATH = "HTML/new_teams.html"
HTML_SPLITTER = "            <tbody>\n"


def main():
    my_interface = Interface(help_user)
    print("Connecting to server. Please wait.")
    try:
        client = SQLClient((raw_input("Enter IP: "), PORT))
        client.connect()
    except socket.error:
        raw_input("Could not connect to server. Press Enter to exit.")
        sys.exit(0)
    print("Connected")
    my_interface.answer("help")
    get_ui, add_ui, update_ui, delete_ui = generate_interfaces(client)
    my_interface.add_request("test", test_server, client)
    my_interface.add_request("get", get, get_ui)
    my_interface.add_request("add", add, add_ui)
    my_interface.add_request("update", update, update_ui)
    my_interface.add_request("delete", delete, delete_ui)
    my_interface.add_request("exit", Interface.default_exit, my_interface)
    loop_interface(my_interface)


def loop_interface(ui):
    """Loops the interface ignoring errors."""
    try:
        ui.loop()
    except socket.error:
        loop_interface(ui)


def generate_interfaces(client):
    """Returns a tuple with: get_interface, add_interface, update_interface, delete_interface."""
    get_interface = Interface(get_help, get_request=lambda: raw_input("Players or Teams? "), back=lambda: None)
    get_interface.add_request("players", get_players, client)
    get_interface.add_request("teams", get_teams, client)

    add_interface = Interface(add_help, get_request=lambda: raw_input("Player or Team? "), back=lambda: None)
    add_interface.add_request("player", add_player, client)
    add_interface.add_request("team", add_team, client)

    update_interface = Interface(update_help, get_request=lambda: raw_input("Player or Team? "), back=lambda: None)
    update_interface.add_request("player", update_player, client)
    update_interface.add_request("team", update_team, client)

    delete_interface = Interface(delete_help, get_request=lambda: raw_input("Player or Team? "), back=lambda: None)
    delete_interface.add_request("player", delete_player, client)
    delete_interface.add_request("team", delete_team, client)

    return get_interface, add_interface, update_interface, delete_interface


def test_server(client):
    print(client.update("Players", 3, first_name="KIKIKLO", last_name="EMMEMES"))


def help_user():
    """Prints helpful information to the user."""
    print("'get' - Get information about players.\n"
          "'add' - Add a player or team.\n"
          "'update' - Update information about a player or team.\n"
          "'delete' - Delete a player or team.\n"
          "'help' - Print this.\n"
          "'exit' - Exit the program.\n")


def get(interface):
    """Get information from the server."""
    interface.answer("help")
    interface.request()


def get_help():
    """Help for get request."""
    print("Enter 'players' to get players or 'teams' to get teams.\n"
          "Enter 'back' to go back.\n")


def get_players(client):
    """Get information on players."""
    print("Press Enter for no filters.\n"
          "Enter the values in the following syntax:\n"
          "name_of_value=value, name_of_value2=value2\n"
          "Values:\n"
          "first_name, last_name, number, age, rings, nationality, team.\n>")
    try:
        values = values_to_dict(raw_input(), "age", "number", "rings")
        try:
            values["team_id"] = values.pop("team")
        except KeyError:
            pass    # User did not enter team.
    except (TypeError, ValueError):
        print("Input is incorrect.")
        return
    real_path = os.path.realpath(HTML_PLAYERS_PATH)
    real_new_path = os.path.realpath(HTML_PLAYERS_NEW_PATH)
    make_html_players(client.receive("Players", **values), client, real_path, real_new_path)
    webbrowser.open_new_tab("file://" + real_new_path)
    raw_input("Please press Enter to continue.")
    remove_file(real_new_path)


def get_teams(client):
    """Get information on teams."""
    print("Press Enter for no filters.\n"
          "Enter the values in the following syntax:\n"
          "name_of_value=value, name_of_value2=value2\n"
          "Values:\n"
          "name, state, city, division, arena, championships, website\n>")
    try:
        values = values_to_dict(raw_input(), "championships")
    except (ValueError, TypeError):
        print("Input is incorrect.")
        return
    real_path = os.path.realpath(HTML_TEAMS_PATH)
    real_new_path = os.path.realpath(HTML_TEAMS_NEW_PATH)
    make_html_teams(client.receive("Teams", **values), real_path, real_new_path)
    webbrowser.open_new_tab("file://" + real_new_path)
    raw_input("Please press Enter to continue.")
    remove_file(real_new_path)


def make_html_players(players, client, read_path=HTML_PLAYERS_PATH, write_path=HTML_PLAYERS_NEW_PATH):
    """Constructs an HTML file with the given players.
    :returns the file test as before changed."""
    with open(read_path, "r") as f:
        opening, ending = f.read().split(HTML_SPLITTER)
    players_string = ""
    for player in players:
        team = html_get_team(player, client)
        players_string += "                <tr id = '{} {}'>\n".format(player.first_name, player.last_name)
        players_string += "                    <td>{}</td>\n".format(player.first_name)
        players_string += "                    <td>{}</td>\n".format(player.last_name)
        players_string += "                    <td>{}</td>\n".format(team)
        players_string += "                    <td>{}</td>\n".format(str(player.number))
        players_string += "                    <td>{}</td>\n".format(str(player.age))
        players_string += "                    <td>{}</td>\n".format(str(player.rings))
        players_string += "                    <td>{}</td>\n".format(player.nationality)
        players_string += "                </tr>\n"
    with open(write_path, "w") as f:
        f.write(opening + HTML_SPLITTER + players_string + ending)


def make_html_teams(teams, read_path=HTML_TEAMS_PATH, write_path=HTML_TEAMS_NEW_PATH):
    with open(read_path, "r") as f:
        opening, ending = f.read().split(HTML_SPLITTER)
    teams_string = ""
    for team in teams:
        teams_string += "                <tr id = '{}'>\n".format(team.name)
        teams_string += "                    <td>{}</td>\n".format(team.name)
        teams_string += "                    <td>{}</td>\n".format(team.state)
        teams_string += "                    <td>{}</td>\n".format(team.city)
        teams_string += "                    <td>{}</td>\n".format(team.division.conference)
        teams_string += "                    <td>{}</td>\n".format(team.division.division)
        teams_string += "                    <td>{}</td>\n".format(team.arena)
        teams_string += "                    <td>{}</td>\n".format(str(team.championships))
        teams_string += "                    <td><a href = '{0}' target = '_blank'>{0}</a></td>\n".format(team.website)
        teams_string += "                </tr>\n"
    with open(write_path, "w") as f:
        f.write(opening + HTML_SPLITTER + teams_string + ending)


def html_get_team(player, client):
    """Returns the team of the player, or 'ERROR'."""
    try:
        return client.receive("TEAMS", id=player.team_id)[0]
    except (IndexError, socket.error):
        return "ERROR"


def remove_file(path):
    """Removes the file and ignores errors."""
    try:
        os.remove(path)
    except (IOError, WindowsError):
        pass


def add(interface):
    """Add a player or team."""
    interface.answer("help")
    interface.request()


def add_help():
    """Help for add request."""
    print("Enter 'player' to add a player or 'team' to add a team.\n"
          "Enter 'back' to go back.\n")


def add_player(client):
    """Add a player."""
    print("Enter the values in the following syntax:\n"
          "name_of_value=value, name_of_value2=value2\n"
          "Values:\n"
          "first_name, last_name, number, age, rings, nationality, team.\n>")
    try:
        values = values_to_dict(raw_input(), "age", "number", "rings")
        values["team_id"] = values.pop("team")
    except (ValueError, TypeError):
        print("Input is incorrect.")
        return
    print(client.add("Players", **values))


def add_team(client):
    """Add a team."""
    print("Enter the values in the following syntax:\n"
          "name_of_value=value, name_of_value2=value2\n"
          "Values:\n"
          "name, state, city, division, arena, championships, website\n>")
    try:
        values = values_to_dict(raw_input(), "championships")
    except (ValueError, TypeError):
        print("Input is incorrect.")
        return
    print(client.add("Teams", **values))


def update(interface):
    """Update a player or team."""
    interface.answer("help")
    interface.request()


def update_help():
    """Help for update request."""
    print("Enter 'player' to update a player or 'team' to update a team.\n"
          "Enter 'back' to go back.\n")


def update_player(client):
    """Update a player."""
    player = get_1_player(client)
    if player is None:
        return
    updates = raw_input("Please enter updates about the player you want to update in the following syntax:\n"
                        "name_of_value=value, name_of_value2=value2\n"
                        "Values:\n"
                        "first_name, last_name, number, age, rings, nationality, team.\n>")
    print(client.update("Players", player.id, **values_to_dict(updates, "number", "age", "rings")))


def update_team(client):
    """Update a team."""
    team = get_1_team(client)
    if team is None:
        return
    updates = raw_input("Please enter updates about the player you want to update in the following syntax:\n"
                        "name_of_value=value, name_of_value2=value2\n"
                        "Values:\n"
                        "name, state, city, division, arena, championships, website\n>")
    print(client.update("Teams", team.id, **values_to_dict(updates, "championships")))


def delete(interface):
    """Delete a player or team."""
    interface.answer("help")
    interface.request()


def delete_help():
    """Help for add request."""
    print("Enter 'player' to delete a player or 'team' to delete a team.\n"
          "Enter 'back' to go back.\n")


def delete_player(client):
    """Delete a player."""
    player = get_1_player(client)
    if player is None:
        return
    if raw_input("Are you sure you want to delete this player (y/n)?") == "y":
        print(client.delete("Players", player.id))
    else:
        print("Cancelling.")


def delete_team(client):
    """Delete a player."""
    team = get_1_team(client)
    if team is None:
        return
    if raw_input("Are you sure you want to delete this team (y/n)?") == "y":
        print(client.delete("Teams", team.id))
    else:
        print("Cancelling.")


def get_1_player(client):
    """Get information from the user to get 1 player from the server.
    Returns the 1 player or None."""
    identify = raw_input("Please enter information about the player you want to update in the following syntax:\n"
                         "name_of_value=value, name_of_value2=value2\n"
                         "Values:\n"
                         "first_name, last_name, number, age, rings, nationality, team.\n>")
    try:
        players = client.receive("Players", **values_to_dict(identify, "number", "age", "rings"))
    except (ValueError, TypeError):
        print("Input was incorrect.")
        return
    if not_1_option(players):
        return
    return players[0]


def get_1_team(client):
    """Get information from the user to get 1 team from the server.
    Returns the 1 player or None."""
    identify = raw_input("Please enter information about the player you want to update in the following syntax:\n"
                         "name_of_value=value, name_of_value2=value2\n"
                         "Values:\n"
                         "name, state, city, division, arena, championships, website\n>")
    try:
        teams = client.receive("Teams", **values_to_dict(identify, "championships"))
    except (ValueError, TypeError):
        print("Input was incorrect.")
        return
    if not_1_option(teams):
        return
    return teams[0]


def not_1_option(options):
    """Prints message and returns True if options is not 1, False otherwise."""
    if len(options) > 1:
        print("More than 1 options fits the description. Please enter more information.")
        return True
    if len(options) == 0:
        print("No options fit the description. Please try again.")
        return True
    return False


def values_to_dict(s, *int_values):
    if s == "":
        return {}
    pairs = s.split(",")
    d = {}
    for pair in pairs:
        key, value = pair.split("=")
        key = key.strip()
        value = value.strip()
        if key in int_values:
            d[key] = int(value)
        else:
            d[key] = value
    return d


class Interface(object):
    """A class that handles the interface with the user."""

    @staticmethod
    def default_get():
        """The default function for get_request."""
        return raw_input("Enter request.\n\n>")

    @staticmethod
    def default_unknown():
        """The default function for unknown_request."""
        print("Unknown request. Please try again.")

    def default_exit(self):
        """The default function for exiting a request or a request loop.
        This is done by turning self.exit to True.
        Change this function by changing the 'exit' request.
        Loop will be broken once the 'exit' request is entered or after the next request after calling this function."""
        self.exit = True

    def __init__(self, help, get_request=default_get.__func__, unknown_request=default_unknown.__func__, **requests):
        """Create a new Interface object.   # __func__ in a staticmethod is the original function.
        help should be callable that prints helpful information to the user.
        get_request - Callable to get a new request.
        unknown_request - Callable to call when an unknown request is received.
        get_request will be called after, to get a new request.
        requests - All other requests to list. Key is a string and value is a callable."""
        self.get_request = get_request
        self.unknown_request = unknown_request
        self.requests = requests
        self.requests["help"] = help
        self.exit = False

    def add_request(self, tag, func, *args, **kwargs):
        """Enter a new request to the list requests. tag is the name and func is the callable.
        If a request with the given tag is already in the list, ValueError is raised."""
        if tag in self.requests:
            raise ValueError("A request with tag '%s' already exists." % tag)
        self.requests[tag] = Request(func, *args, **kwargs)

    def del_request(self, tag):
        """Delete a request from the list of requests.
        If a request with the given tag is not in the list, ValueError is raised."""
        try:
            del self.requests[tag]
        except KeyError:
            raise ValueError("A request with tag '%s' does not exist." % tag)

    def change_request(self, tag, func, *args, **kwargs):
        """Change request [tag] to call [func] instead of the previously bound callable.
        If a request with the given tag is not in the list, ValueError is raised."""
        if tag not in self.requests:
            raise ValueError("A request with tag '%s' does not exist." % tag)
        self.requests[tag] = Request(func, *args, **kwargs)

    def request(self):
        """Answers the request based on the tags given."""
        self.answer(self.get_request())

    def answer(self, request):
        """Answers a request."""
        try:
            self.requests[request]()
        except KeyError:
            self.unknown_request()
            self.request()

    def loop(self):
        """Will enter a loop or getting a request, answering it, and getting another request.
        Loop can be broken using a request that exists the loop."""
        while not self.exit:
            self.request()


class Request(object):
    """A request to be called by Requests."""
    def __init__(self, func, *args, **kwargs):
        """Create a new Request function."""
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        """Called to run self.func."""
        self.func(*self.args, **self.kwargs)


if __name__ == "__main__":
    main()
