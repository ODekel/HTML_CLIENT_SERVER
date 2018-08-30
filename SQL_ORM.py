import sqlite3
import copy


class LimitError(Exception):
    """An exception to show an ID object is over its limit."""
    pass


class ID(object):
    """Used to create primary keys in SQL."""
    def __init__(self, start=1, limit=None, jump=1):
        """Create a new object that will returns different primary keys each time.
        Use start to start at a number other than 1.
        Use limit raise a LimitError if get_next is called and
        the next ID is over (or under, for negative a 'jump') 'limit'.
        Use jump to change the difference between each ID."""
        self._current = start
        self._limit = limit
        if jump == 0:
            raise ValueError("'jump' cannot be 0.")
        self._jump = jump

    @property
    def limit(self):
        """Get the limit for the ID."""
        return self._limit

    @property
    def jump(self):
        """Get the jump for the ID."""
        return self._jump

    def get_next(self):
        """Get the next ID."""
        ret_current = copy.copy(self._current)
        self._current += self._jump

        if self._limit is None:
            return ret_current

        if self._jump > 0:
            if ret_current > self._limit:
                raise LimitError("Next ID is over the limit.")
            return ret_current

        if self._jump < 0:
            if ret_current < self._limit:
                raise LimitError("Next ID is under the limit.")
            return ret_current

        else:   # Should not be able to get here, means self._jump = 0.
            raise ValueError("'jump' is equal to 0.")


class Conference(object):
    """A state class."""
    Western = "Western"
    Eastern = "Eastern"


class Division(object):
    """A data holder class class."""
    Pacific = "Pacific"
    Southwest = "Southwest"
    Northwest = "Northwest"
    Atlantic = "Atlantic"
    Central = "Central"
    Southeast = "Southeast"

    def __init__(self, division):
        """division must be one of the options that come with the class, else ValueError will be raised."""
        if division in (Division.Pacific, Division.Southwest, Division.Northwest):
            self.division = division
            self.conference = Conference.Western
        elif division in (Division.Atlantic, Division.Central, Division.Southeast):
            self.division = division
            self.conference = Conference.Eastern
        else:
            raise (ValueError, "Division does not exist")


class Team(object):
    """A class representing an NBA team."""
    def __init__(self, primary, name, state, city, division, arena, championships, website):
        """Create a Team object. All players' team attribute will be set to self."""
        self._id = primary

        # self._logo = logo
        # self._logo = None    # logo not supported yet.

        self._name = self.name = name
        self._state = self.state = state
        self._city = self.city = city
        self._division = self.division = division
        self._arena = self.arena = arena
        self._championships = self.championships = championships
        self._website = self.website = website

    # @staticmethod
    # def _check_players_init(players):
    #     """Checks if all the players in the list are of the Player type, else raises TypeError."""
    #     for player in players:
    #         if not isinstance(player, Player):
    #             raise TypeError("All players in a team must be an object of the Player class.")
    #     return True

    def __str__(self):
        """A readable representation of the class."""
        return self.name

    @property
    def id(self):
        """ID of the Team."""
        return self._id

    @id.setter
    def id(self, value):
        if not isinstance(value, int) or value is None:
            raise TypeError("ID MUST be an int!")
        self._id = value

    @property
    def name(self):
        """Name of the team."""
        return self._name

    @name.setter
    def name(self, value):
        if not isinstance(value, basestring):
            raise TypeError("name MUST be a string!")
        self._name = value

    @property
    def state(self):
        """State the team is from."""
        return self._state

    @state.setter
    def state(self, value):
        if not isinstance(value, basestring):
            raise TypeError("state MUST be a string!")
        self._state = value

    @property
    def city(self):
        """City the team is from."""
        return self._city

    @city.setter
    def city(self, value):
        if not isinstance(value, basestring):
            raise TypeError("city MUST be a string!")
        self._city = value

    @property
    def division(self):
        """Division the team is part of."""
        return self._division

    @division.setter
    def division(self, value):
        if isinstance(value, basestring):
            d_value = Division(value)
        else:
            d_value = value
        if not isinstance(d_value, Division):
            raise TypeError("division MUST be an object of the Division class or a string that can be turned into one!")
        self._division = d_value

    @property
    def arena(self):
        """Arena the team is playing at."""
        return self._arena

    @arena.setter
    def arena(self, value):
        if not isinstance(value, basestring):
            raise TypeError("arena MUST be a string!")
        self._arena = value

    @property
    def championships(self):
        """Number of championships the team has won."""
        return self._championships

    @championships.setter
    def championships(self, value):
        if not isinstance(value, int):
            raise TypeError("championships MUST be an int!")
        self._championships = value

    @property
    def website(self):
        """Official website of the team."""
        return self._website

    @website.setter
    def website(self, value):
        if not isinstance(value, basestring):
            raise TypeError("website MUST be a string!")
        self._website = value

    # @property
    # def logo(self):
    #     """Official logo of the team."""
    #     return self._logo


class Player(object):
    """A class representing an NBA player."""
    def __init__(self, primary, first_name, last_name, number, age, rings, nationality, team_id):
        """Create a Player object."""
        self._id = primary

        # self._image = image
        # self._image = None   # image not yet supported.

        self._first_name = self.first_name = first_name
        self._last_name = self.last_name = last_name
        self._team_id = self.team_id = team_id
        self._number = self.number = number
        self._age = self.age = age
        self._rings = self.rings = rings
        self._nationality = self.nationality = nationality

    def __str__(self):
        """A readable representation of the class."""
        return "%s %s" % (self.first_name, self.last_name)

    @property
    def id(self):
        """ID of the player."""
        return self._id

    @id.setter
    def id(self, value):
        if not isinstance(value, int) or value is None:
            raise TypeError("ID MUST be an int!")
        self._id = value

    @property
    def first_name(self):
        """First name of the player."""
        return self._first_name

    @first_name.setter
    def first_name(self, value):
        if not isinstance(value, basestring):
            raise TypeError("first_name MUST be a string!")
        self._first_name = value

    @property
    def last_name(self):
        """Last name of the player."""
        return self._last_name

    @last_name.setter
    def last_name(self, value):
        if not isinstance(value, basestring):
            raise TypeError("last_name MUST be a string!")
        self._last_name = value

    @property
    def team_id(self):
        """Team the player is playing for."""
        return self._team_id

    @team_id.setter
    def team_id(self, value):
        if not isinstance(value, int):
            raise TypeError("team_id MUST be an integer.")
        self._team_id = value

    @property
    def number(self):
        """Number of the player."""
        return self._number

    @number.setter
    def number(self, value):
        if not isinstance(value, int):
            raise TypeError("number MUST be an int!")
        self._number = value

    @property
    def age(self):
        """Age of the player."""
        return self._age

    @age.setter
    def age(self, value):
        if not isinstance(value, int):
            raise TypeError("age MUST be an int!")
        self._age = value

    @property
    def rings(self):
        """Number of rings the player has won in his career."""
        return self._rings

    @rings.setter
    def rings(self, value):
        if not isinstance(value, int):
            raise TypeError("rings MUST be an integer!")
        self._rings = value

    @property
    def nationality(self):
        """Nationality of the player."""
        return self._nationality

    @nationality.setter
    def nationality(self, value):
        if not isinstance(value, basestring):
            raise TypeError("nationality MUST be a string!")
        self._nationality = value

    # @property
    # def image(self):
    #     """Image of the player."""
    #     return self._image


class ORM(object):
    def __init__(self, db_name="ORM"):
        self.conn = None  # will store the DB connection
        self.cursor = None   # will store the DB connection cursor
        self.db_name = db_name  # The name of the data base with no .db at the end.
        self.team = TeamORM(self)
        self.player = PlayerORM(self)
        self.start_db()

    def start_db(self):
        """Manages the opening of a the DB."""
        self.open()
        # self.conn.create_function("dbo.TeamNumberConstraint", 2, self.player.team_number_constraint)
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Teams" +
                            "(" +
                            "id INTEGER PRIMARY KEY," +
                            "name TEXT UNIQUE," +
                            "state TEXT," +
                            "city TEXT," +
                            "division TEXT," +
                            "arena TEXT," +
                            "championships INTEGER," +
                            "website TEXT" +
                            ");")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS Players" +
                            "(" +
                            "id INTEGER PRIMARY KEY," +
                            "first_name TEXT," +
                            "last_name TEXT,"
                            "number INTEGER," +
                            "age INTEGER," +
                            "rings INTEGER," +
                            "nationality TEXT," +
                            "team_id INTEGER," +
                            "FOREIGN KEY(team_id) REFERENCES Teams(id)" +
                            ");")
        self.commit()
        self.close()

    def open(self):
        """
        will open DB file and put value in:
        self.conn (need DB file name)
        and self.cursor
        """
        self.conn = sqlite3.connect(self.db_name + ".db")
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA foreign_keys = ON;")
        
    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    @staticmethod
    def constraints_to_tuples(**constraints):
        """Turns constraints received with ** into 2 tuples that can be used to perform sql queries.
        Use tuple[0] (columns) with string.format, and tuple[1] (values) with sqlite3's ?."""
        columns_list = []
        values_list = []
        for constraint in constraints.iteritems():
            columns_list.append(constraint[0])
            values_list.append(constraint[1])
        columns_tuple = tuple(columns_list)
        values_tuple = tuple(values_list)
        return columns_tuple, values_tuple

    @staticmethod
    def constraints_to_tuples_contains(**constraints):
        """Does the same thing as constraints_to_tuples, but adds %value% to the values."""
        columns_list = []
        values_list = []
        for constraint in constraints.iteritems():
            columns_list.append(constraint[0])
            values_list.append("%{}%".format(constraint[1]))
        columns_tuple = tuple(columns_list)
        values_tuple = tuple(values_list)
        return columns_tuple, values_tuple

    def select(self, query, func=None, values=()):
        """Executes the query (using sqlite's second tuple parameter to replace ?s with values),
        calls func (callable) with each item the query returns and returns a list with all the results of func.
        Use this to retrieve information from the DB."""
        self.open()
        cursor_objs = self.cursor.execute(query, values)
        if func is not None:
            cursor_objs = [func(obj) for obj in cursor_objs]
        self.close()
        return cursor_objs

    def change(self, query, values=()):
        """Executes the query (using sqlite's second tuple parameter to replace ?s with values) and commits.
        No value is returned and there is no protection against crashes, so you can handle it yourself.
        Use this to make changes to the DB."""
        self.open()
        self.cursor.execute(query, values)
        self.commit()
        self.close()

    def get(self, table, func=None, ratio="=", **constraints):
        """Return a list with all the [table] where all constraints[0] = constraints[1].
        e.g. get("Teams", state='California') will return a list with all the teams
        where team.state is equal to 'California'.
        Does not accept not built-in objects.
        ratio - '=' is equal to, '<' is smaller than, '>' is bigger than."""
        if not constraints:
            return self._get_no_constraints(table, func)
        qmarks = (("{} %s ? AND " % ratio) * len(constraints))[:-5]    # ? auto adds '' to the word.
        query = "SELECT * FROM {} WHERE ".format(table) + qmarks + ";"
        columns_tuple, values_tuple = ORM.constraints_to_tuples(**constraints)
        return self.select(query.format(*columns_tuple), func=func, values=values_tuple)

    def get_contains(self, table, func=None, **constraints):
        """Does the same thing as get, but instead of checking whether for equals,
        it checks whether or not the column contains the value.
        All values must be strings."""
        if not constraints:
            return self._get_no_constraints(table)
        qmarks = ("{} LIKE ? AND " * len(constraints))[:-5]  # ? auto adds '' to the word.
        query = "SELECT * FROM {} WHERE ".format(table) + qmarks + ";"
        columns_tuple, values_tuple = ORM.constraints_to_tuples_contains(**constraints)
        return self.select(query.format(*columns_tuple), func=func, values=values_tuple)

    def _get_no_constraints(self, table, func=None):
        """When self.get is called with no parameters, this function is called."""
        return self.select("SELECT * FROM {};".format(table), func=func)

    # WRITE

    def add(self, table, obj=None, **values):
        """Inserts into the DB.
        If obj is given and not None, vars() is used and values is ignored.
        Else, values must be given and will be used.
        Returns True for success and False for failure to update the DB."""
        if obj is None and not values:
            raise ValueError("Values or obj MUST be passed.")
        if obj is not None:
            values = vars(obj)
        columns_tuple, values_tuple = ORM.constraints_to_tuples(**values)
        try:
            self.change("INSERT INTO {} ".format(table) +
                        ("(" + ("{}, " * len(columns_tuple))[:-2] + ")").format(*columns_tuple) +
                        "VALUES (" + ("?, " * len(values_tuple))[:-2] + ");", values_tuple)
        except sqlite3.Error:
            return False
        return True

    def update(self, table, (id_column, id_value), obj=None, **updates):
        """Updates the DB (not the object!) about obj.
        If obj is given and not None, vars() is used and updates is ignored.
        id column is a string with the name of the column and value is the value.
        Returns True for success and False for failure to update the DB."""
        if obj is None and not updates:
            raise ValueError("Updates or obj must be given.")
        qmarks = ("{} = ?, " * len(updates))[:-2]  # ? auto adds '' to the word.
        query = "UPDATE {} SET ".format(table) + qmarks + "WHERE {} = {}".format(id_column, id_value)
        if obj is not None:
            updates = vars(obj)
        columns_tuple, values_tuple = ORM.constraints_to_tuples(**updates)
        try:
            if id_column in columns_tuple and values_tuple[columns_tuple.index(id_column)] != id_value:
                raise sqlite3.Error    # No updating the id.
            self.change(query.format(*columns_tuple), values_tuple)
        except sqlite3.Error:
            return False
        return True

    def delete(self, table, (id_column, id_value)):
        """Deletes the row from [table] WHERE id_column=id_value.
        Returns True for success and False for failure to delete from the DB."""
        try:
            self.change("DELETE FROM {} WHERE {}=?".format(table, id_column), (id_value, ))
        except sqlite3.Error:
            return False
        return True


class TeamORM(object):
    """SQL commands to use with Team class."""
    def __init__(self, orm):
        """Will use the ORM object provided to execute the SQL commands."""
        self.orm = orm

    @staticmethod
    def sql_to_object(sql):
        """sql is the list of values returned from the db.
        Will return a Team object."""
        return Team(*sql)

    @staticmethod
    def object_to_sql(obj):
        """obj is a Team object.
        Will return a tuple that can be used with sql and the db."""
        return obj.id, obj.name, obj.state, obj.city, obj.division.division, obj.arena, obj.championships, obj.website
        # obj.logo

    @staticmethod
    def object_to_dict(obj):
        """Basically like vars(), but cuts the underscores at the start of attribute names."""
        return {"id": obj.id, "name": obj.name, "state": obj.state, "city": obj.city,
                "division": obj.division.division, "arena": obj.arena, "championships": obj.championships,
                "website": obj.website}

    @staticmethod
    def dict_to_object(d):
        """The reverse of object_to_dict."""
        return Team(d["id"], d["name"], d["state"], d["city"], d["division"], d["arena"], d["championships"],
                    d["website"])

    def get_teams(self, ratio="=", **constraints):
        """Return a list with all the teams where all constraints[0] = constraints[1].
        e.g. get_teams(state='California') will return a list with all the teams
        where team.state is equal to 'California'.
        Does not accept not built-in objects (i.e. not Division objects, use Division.division instead).
        ratio - '=' is equal to, '<' is smaller than, '>' is bigger than."""
        return self.orm.get("Teams", TeamORM.sql_to_object, ratio, **constraints)

    def get_teams_contains(self, **constraints):
        """Does the same thing as get_teams, but instead of checking whether for equals,
        it checks whether or not the column contains the value.
        All values must be strings."""
        return self.orm.get_contains("Teams", TeamORM.sql_to_object, **constraints)

    def _get_teams_no_constraints(self):
        """When self.get_teams is called with no parameters, this function is called."""
        return self.orm.get("Teams", TeamORM.sql_to_object)

    # WRITE

    def add_team(self, team):
        """Inserts the Team object into the DB.
        Returns True for success and False for failure to update the DB."""
        return self.orm.add("Teams", **TeamORM.object_to_dict(team))

    def update_team(self, team, **updates):
        """Updates the DB (not the object!) about the team.
        Example: update_team(bulls, division=some_new_division).
        Returns True for success and False for failure to update the DB."""
        if isinstance(team, Team):
            return self.orm.update("Teams", ("id", team.id), **updates)
        if isinstance(team, int):
            return self.orm.update("Teams", ("id", team), **updates)
        raise TypeError("team MUST be a Team object or a team id.")

    def delete_team(self, team):
        """Delete a team from the DB.
        Returns True for success and False for failure to update the DB."""
        if isinstance(team, Team):
            return self.orm.delete("Teams", ("id", team.id))
        if isinstance(team, int):
            return self.orm.delete("Teams", ("id", team))
        raise TypeError("team MUST be a Team object or a team id.")


class PlayerORM(object):
    """SQL commands to use with Player class."""
    def __init__(self, orm):
        """Will use the ORM object provided to execute the SQL commands."""
        self.orm = orm

    @staticmethod
    def sql_to_object(sql):
        """sql is the list of values returned from the db.
        Will return a Player object."""
        return Player(*sql)

    @staticmethod
    def object_to_sql(obj):
        """obj is a Player object.
        Will return a tuple that can be used with sql and db."""
        return obj.id, obj.first_name, obj.last_name, obj.number, obj.age, obj.rings,\
            obj.nationality, obj.team_id    # , obj.image

    @staticmethod
    def object_to_dict(obj):
        """Basically like vars(), but cuts the underscores at the start of attribute names."""
        return {"id": obj.id, "first_name": obj.first_name, "last_name": obj.last_name, "number": obj.number,
                "age": obj.age, "rings": obj.rings, "nationality": obj.nationality, "team_id": obj.team_id}

    @staticmethod
    def dict_to_object(d):
        """The reverse of object_to_dict."""
        return Player(d["id"], d["first_name"], d["last_name"], d["number"], d["age"], d["rings"], d["nationality"],
                      d["team_id"])

    def get_players(self, ratio="=", **constraints):
        """See TeamORM.get_teams help."""
        return self.orm.get("Players", PlayerORM.sql_to_object, ratio, **constraints)

    def get_players_contains(self, **constraints):
        """See PlayerORM.get_teams_contains help."""
        return self.orm.get_contains("Players", PlayerORM.sql_to_object, **constraints)

    def _get_players_no_constraints(self):
        """When self.get_players is called with no parameters, this function is called."""
        return self.orm.get("Players", PlayerORM.sql_to_object)

    # WRITE

    def add_player(self, player):
        """See TeamORM.add_team help."""
        return self.orm.add("Players", **PlayerORM.object_to_dict(player))

    def update_player(self, player, **updates):
        """See TeamORM.update_team help."""
        if isinstance(player, Player):
            return self.orm.update("Players", ("id", player.id), **updates)
        if isinstance(player, int):
            return self.orm.update("Players", ("id", player), **updates)
        raise TypeError("player MUST be a Player object or a player id.")

    def delete_player(self, player):
        """Delete a player from the DB.
        Returns True for success and False for failure to update the DB."""
        if isinstance(player, Player):
            return self.orm.delete("Players", ("id", player.id))
        if isinstance(player, int):
            return self.orm.delete("Players", ("id", player))
        raise TypeError("player MUST be a Player object or a player id.")
