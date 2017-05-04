from __future__ import division

import datetime
from collections import Counter
from bandicoot_dev.helper.tools import Colors
from bandicoot_dev.helper.group import _binning
import bandicoot_dev as bc


class Record(object):
    """
    Data structure storing a call detail record.

    Attributes
    ----------
    interaction : str, 'text' or 'call'
        The type of interaction (text message or call).
    direction : str, 'in' or 'out'
        The direction of the record (incoming or outgoing).
    correspondent_id : str
        A unique identifier for the corresponding contact
    datetime : datetime
        The exact date and time of the interaction
    duration : int or None
        Durations of the call in seconds. None if the record is a text message.
    position : Position
        The geographic position of the user at the time of the interaction.
    """

    def __init__(self, **kwargs):
        self.parameters = kwargs.keys()
        for kw, arg in kwargs.items():
            setattr(self, kw, arg)

    def __repr__(self):
        return "Record(" + ", ".join(map(lambda x: "%s=%r" % (x, getattr(self, x)), self.parameters)) + ")"

    def __eq__(self, other):
        if isinstance(other, self.__class__) and self.parameters == other.parameters:
            return all(getattr(self, attr) == getattr(other, attr) for attr in self.parameters)
        return False

    def __hash__(self):
        return hash(self.__repr__())

    def matches(self, other):
        """
        Returns true if two records 'match' - that is, they correspond to the same event from two perspectives.
        """
        return self.interaction == other.interaction and \
            self.direction != other.direction and \
            self.duration == other.duration and \
            abs((self.datetime - other.datetime).total_seconds()) < 30

    def all_matches(self, iterable):
        return filter(self.matches, iterable)

    def has_match(self, iterable):
        return len(self.all_matches(iterable)) > 0


class Position(object):
    """
    Data structure storing a generic location. Can be instantiated with either a
    stop or a gps location. Printing out the position will show which was used
    to instantiate it.
    """

    def __init__(self, stop=None, location=None):
        self.stop = stop
        self.location = location

    def _get_location(self, user):
        if self.location:
            return self.location
        elif self.stop:
            return user.stops.get(self.stop)
        else:
            return None

    def type(self):
        if self.stop:
            return 'stop'
        else:
            return 'gps'

    def __repr__(self):
        if self.stop and self.location:
            return "Position(stop=%s, location=%s)" % (self.stop, self.location)
        if self.stop:
            return "Position(stop=%s)" % self.stop
        if self.location:
            return "Position(location=(%s, %s))" % self.location

        return "Position()"

    def __eq__(self, other):
        if not isinstance(other, Position):
            return False
        if self.stop and other.stop:
            return self.stop == other.stop
        if self.location and other.location:
            return self.location == other.location

        if not self.location and not self.stop and not other.location and not other.stop:
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__repr__())


class User(object):
    """
    Data structure storing all the call, text or mobility records of the user.
    """

    def __init__(self):
        self._call_records = []
        self._text_records = []
        self._physical_records = []
        self._screen_records = []
        self._stop_records = []
        self._stops = {}

        self.name = None
        self.stops_path = None
        self.attributes_path = None

        self.start_time = {"any": None, "call": None, "text": None, "physical": None, "screen": None, "stop": None}
        self.end_time = {"any": None, "call": None, "text": None, "physical": None, "screen": None, "stop": None}
        self.night_start = datetime.time(22)  # bandicoot orig. is 19!
        self.night_end = datetime.time(7)
        self.weekend = [6, 7]  # Saturday, Sunday by default

        self.home = None
        self.supported_types = {
            "text": False,
            "call": False,
            "physical": False,
            "screen": False,
            "stop": False,
            "stops": False
        }

        self.attributes = {}
        self.ignored_records = {
            "text": None,
            "call": None,
            "physical": None,
            "screen": None,
            "stop": None,
            "stops": None
        }

        self.percent_outofnetwork_calls = 0
        self.percent_outofnetwork_texts = 0
        self.percent_outofnetwork_contacts = 0
        self.percent_outofnetwork_call_durations = 0

        self.network = {}


    def update_time_any(self, startorend, t):
        if startorend == "start": 
            if self.start_time["any"] == None or t < self.start_time["any"]:
                self.start_time["any"] = t
        if startorend == "end":
            if self.end_time["any"] == None or t > self.end_time["any"]:
                self.end_time["any"] = t


    @property
    def call_records(self):
        return self._call_records

    @call_records.setter
    def call_records(self, input):
        self._call_records = sorted(input, key=lambda r: r.datetime)
        if len(self._call_records) > 0:
            self.start_time['call'] = self._call_records[0].datetime
            self.end_time['call'] = self._call_records[-1].datetime
            self.update_time_any("start", self.start_time['call'])
            self.update_time_any("end", self.end_time['call'])
            self.supported_types['call'] = True

    @property
    def text_records(self):
        return self._text_records

    @text_records.setter
    def text_records(self, input):
        self._text_records = sorted(input, key=lambda r: r.datetime)
        if len(self._text_records) > 0:
            self.start_time['text'] = self._text_records[0].datetime
            self.end_time['text'] = self._text_records[-1].datetime
            self.update_time_any("start", self.start_time['text'])
            self.update_time_any("end", self.end_time['text'])
            self.supported_types['text'] = True

    @property
    def physical_records(self):
        return self._physical_records

    @physical_records.setter
    def physical_records(self, input):
        self._physical_records = sorted(input, key=lambda r: r.datetime)
        if len(self._physical_records) > 0:
            self.start_time['physical'] = self._physical_records[0].datetime
            self.end_time['physical'] = self._physical_records[-1].datetime
            self.update_time_any("start", self.start_time['physical'])
            self.update_time_any("end", self.end_time['physical'])
            self.supported_types['physical'] = True

    @property
    def screen_records(self):
        return self._screen_records

    @screen_records.setter
    def screen_records(self, input):
        self._screen_records = sorted(input, key=lambda r: r.datetime)
        if len(self._screen_records) > 0:
            self.start_time['screen'] = self._screen_records[0].datetime
            self.end_time['screen'] = self._screen_records[-1].datetime
            self.update_time_any("start", self.start_time['screen'])
            self.update_time_any("end", self.end_time['screen'])
            self.supported_types['screen'] = True

    @property
    def stop_records(self):
        return self._stop_records

    @stop_records.setter
    def stop_records(self, input):
        self._stop_records = sorted(input, key=lambda r: r.datetime)
        if len(self._stop_records) > 0:
            self.start_time['stop'] = self._stop_records[0].datetime
            self.end_time['stop'] = self._stop_records[-1].datetime
            self.update_time_any("start", self.start_time['stop'])
            self.update_time_any("end", self.end_time['stop'])
            self.supported_types['stop'] = True

        #self.recompute_home()

    @property
    def stops(self):
        """
        The purpose of this is to hook into assignments to the
        user's stop dictionary, and update records' location
        based on the new value.
        """
        return self._stops

    @stops.setter
    def stops(self, input_):
        self._stops = input_
        self.supported_types['stops'] = len(input_) > 0
        for r in self._stop_records:
            if r.position.stop in self._stops:
                r.position.location = self._stops[r.position.stop]
            else:
                r.position.location = None

    def recompute_missing_neighbors(self):
        """
        Recomputes statistics for missing users of the current user's
        network:

        - ``User.percent_outofnetwork_calls``
        - ``User.percent_outofnetwork_texts``
        - ``User.percent_outofnetwork_contacts``
        - ``User.percent_outofnetwork_call_durations``

        This function is automatically called from :meth:`~bandicoot.io.read_csv`
        when loading a network user.
        """

        oon_records = [r for r in self.records if self.network.get(r.correspondent_id, None) is None]
        num_oon_calls = len([r for r in oon_records if r.interaction == 'call'])
        num_oon_texts = len([r for r in oon_records if r.interaction == 'text'])
        num_oon_neighbors = len(set(x.correspondent_id for x in oon_records))
        oon_call_durations = sum([r.duration for r in oon_records if r.interaction == 'call'])

        num_calls = len([r for r in self.records if r.interaction == 'call'])
        num_texts = len([r for r in self.records if r.interaction == 'text'])
        total_neighbors = len(set(x.correspondent_id for x in self.records))
        total_call_durations = sum([r.duration for r in self.records if r.interaction == 'call'])

        def _safe_div(a, b, default):
            return a / b if b != 0 else default

        # We set the percentage at 0 if no event occurs
        self.percent_outofnetwork_calls = _safe_div(num_oon_calls, num_calls, 0)
        self.percent_outofnetwork_texts = _safe_div(num_oon_texts, num_texts, 0)
        self.percent_outofnetwork_contacts = _safe_div(num_oon_neighbors, total_neighbors, 0)
        self.percent_outofnetwork_call_durations = _safe_div(oon_call_durations, total_call_durations, 0)

    def describe(self):
        """
        Generate a short description of the object.

        Examples
        --------
        >>> import bandicoot_dev as bc
        >>> user = bc.User()
        >>> user.records = bc.tests.generate_user.random_burst(5)
        >>> user.describe()
        [x] 5 records from 2014-01-01 10:41:00 to 2014-01-01 11:21:00
            5 contacts
        [x] 1 attribute

        Note
        ----
        The summary is directly sent to the standard output.
        """

        def format_int(name, n):
            if n == 0 or n == 1:
                return "%i %s" % (n, name[:-1])
            else:
                return "%i %s" % (n, name)

        empty_box = Colors.OKGREEN + '[ ]' + Colors.ENDC + ' '
        filled_box = Colors.OKGREEN + '[x]' + Colors.ENDC + ' '

        if self.start_time['call'] is None:
            print empty_box + "No call_records stored"
        else:
            print (filled_box + format_int("call_records", len(self.call_records)) +
                   " from %s to %s" % (self.start_time['call'], self.end_time['call']))
        if self.start_time['text'] is None:
            print empty_box + "No text_records stored"
        else:
            print (filled_box + format_int("text_records", len(self.text_records)) +
                   " from %s to %s" % (self.start_time['text'], self.end_time['text']))
        if self.start_time['physical'] is None:
            print empty_box + "No physical_records stored"
        else:
            print (filled_box + format_int("physical_records", len(self.physical_records)) +
                   " from %s to %s" % (self.start_time['physical'], self.end_time['physical']))
        if self.start_time['screen'] is None:
            print empty_box + "No screen_records stored"
        else:
            print (filled_box + format_int("screen_records", len(self.screen_records)) +
                   " from %s to %s" % (self.start_time['screen'], self.end_time['screen']))
        if self.start_time['stop'] is None:
            print empty_box + "No stop_records stored"
        else:
            print (filled_box + format_int("stop_records", len(self.stop_records)) +
                   " from %s to %s" % (self.start_time['stop'], self.end_time['stop']))

        nb_contacts = bc.individual.number_of_contacts(self, interaction='callandtextandphysical', groupby=None)
        nb_contacts = nb_contacts['allweek']['allday']['call+text+physical']
        if nb_contacts:
            print filled_box + format_int("contacts", nb_contacts)
        else:
            print empty_box + "No contacts"

        if self.attributes:
            print filled_box + format_int("attributes", len(self.attributes))
        else:
            print empty_box + "No attribute stored"

        if self.supported_types['text']:
            print filled_box + "Has texts"
        else:
            print empty_box + "No texts"

        if self.supported_types['call']:
            print filled_box + "Has calls"
        else:
            print empty_box + "No calls"

        if self.network:
            print filled_box + "Has network"
        else:
            print empty_box + "No network"

    def recompute_home(self):
        """
        Return the stop where the user spends most of his time at night.
        None is returned if there are no candidates for a home stop
        """

        if self.night_start < self.night_end:
            night_filter = lambda r: self.night_end > r.datetime.time() > self.night_start
        else:
            night_filter = lambda r: not(self.night_end < r.datetime.time() < self.night_start)

        # Bin positions by chunks of 30 minutes
        candidates = list(_binning(filter(night_filter, self._records)))

        if len(candidates) == 0:
            self.home = None
        else:
            self.home = Counter(candidates).most_common()[0][0]

        return self.home

    @property
    def has_home(self):
        return self.home is not None

    @property
    def has_attributes(self):
        return len(self.attributes) != 0

    @property
    def has_network(self):
        return self.network != {}

    def set_home(self, new_home):
        """
        Sets the user's home. The argument can be a Position object or a
        tuple containing location data.
        """
        if type(new_home) is Position:
            self.home = new_home

        elif type(new_home) is tuple:
            self.home = Position(location=new_home)

        else:
            self.home = Position(stop=new_home)
