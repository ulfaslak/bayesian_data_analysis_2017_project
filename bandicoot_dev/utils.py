from bandicoot_dev.helper.tools import OrderedDict, warning_str, Inc_avg
from bandicoot_dev.helper.group import group_records, DATE_GROUPERS
import bandicoot_dev as bc

from functools import partial
from datetime import datetime as dt


def flatten(d, parent_key='', separator='__'):
    """
    Flatten a nested dictionary.

    Parameters
    ----------
    d: dict_like
        Dictionary to flatten.
    parent_key: string, optional
        Concatenated names of the parent keys.
    separator: string, optional
        Separator between the names of the each key.
        The default separator is '_'.

    Examples
    --------

    >>> d = {'alpha': 1, 'beta': {'a': 10, 'b': 42}}
    >>> flatten(d) == {'alpha': 1, 'beta_a': 10, 'beta_b': 42}
    True
    >>> flatten(d, separator='.') == {'alpha': 1, 'beta.a': 10, 'beta.b': 42}
    True

    """
    items = []
    for k, v in d.items():
        new_key = parent_key + separator + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, separator).items())
        else:
            items.append((new_key, v))
    return OrderedDict(items)


def all(user, groupby='week', summary='default', dist=False, network=False, spatial=False, split_week=False, split_day=False, attributes=True, flatten=False):
    """
    Returns a dictionary containing all bandicoot indicators for the user,
    as well as reporting variables.

    Relevant indicators are defined in the 'individual', and 'spatial' modules.

    =================================== =======================================================================
    Reporting variables                 Description
    =================================== =======================================================================
    attributes_path                     directory where attributes were loaded
    version                             bandicoot version
    groupby                             grouping method ('week' or None)
    split_week                          whether or not indicators are also computed for weekday and weekend
    split_day                           whether or not indicators are also computed for day and night
    start_time                          time of the first record
    end_time                            time of the last record
    night_start, night_end              start and end time to define nights
    weekend                             days used to define the weekend (``[6, 7]`` by default, where 1 is Monday)
    bins                                number of weeks if the record are grouped
    has_call                            whether or not records include calls
    has_text                            whether or not records include texts
    has_home                            whether or not a :meth:`home location <bandicoot.core.User.recompute_home>` has been found
    has_network                         whether or not correspondents where loaded
    percent_outofnetwork_calls          percentage of calls, received or emitted, made with a correspondant not loaded in the network
    percent_outofnetwork_texts          percentage of texts with contacts not loaded in the network
    percent_outofnetwork_contacts       percentage of contacts not loaded in the network
    percent_outofnetwork_call_durations percentage of minutes of calls where the contact was not loaded in the network
    number_of_records                   total number of records
    =================================== =======================================================================

    We also include a last set of reporting variables, for the records ignored
    at load-time. Values can be ignored due to missing or inconsistent fields  
    (e.g., not including a valid 'datetime' value).  

    .. code-block:: python

        {
            'all': 0,
            'interaction': 0,
            'direction': 0,
            'correspondent_id': 0,
            'datetime': 0,
            'duration': 0
        }

    with the total number of records ignored (key ``'all'``), as well as the
    number of records with faulty values for each columns.
    """

    # Warn the user if they are selecting weekly and there's only one week
    if groupby is not None:
        record_types = [user.call_records, user.text_records, user.physical_records, user.screen_records, user.stop_records]
        for records in record_types:
            if len(records) < 1:
                continue
            if len(set(DATE_GROUPERS[groupby](r.datetime) for r in records)) <= 1:
                print warning_str('Grouping by {0}, but all data is from the same {0}!'.format(groupby))
    scalar_type = 'distribution_scalar' if not dist else 'scalar'
    summary_type = 'distribution_summarystats' if not dist else 'summarystats'

    individual_functions = [
        #(bc.individual.active_days, scalar_type),
        (bc.individual.number_of_contacts, scalar_type),  # v
        (bc.individual.number_of_interactions, scalar_type),  # v
        #(bc.individual.entropy, scalar_type),  # x
        #(bc.individual.interactions_per_contact, scalar_type),  # x
        #(bc.individual.percent_ei_percent_interactions, scalar_type),  # v
        (bc.individual.percent_ei_percent_durations, scalar_type),  # v
        (bc.individual.balance_of_interactions, scalar_type),  # v
        (bc.individual.duration, scalar_type),  # v
        (bc.individual.percent_initiated_conversations, summary_type),  # v
        (bc.individual.percent_concluded_conversations, summary_type),  # v
        (bc.individual.overlap_conversations, scalar_type),  # v
        (bc.individual.response_delay, summary_type),  # v
        (bc.individual.response_rate, summary_type),  # v
        (bc.individual.percent_nocturnal, scalar_type),  # v
        (bc.individual.interevent_time, scalar_type),  # v
        (bc.individual.ratio_social_screen_alone_screen, scalar_type),  # v
        (bc.individual.ratio_interactions_campus_other, scalar_type),  # v
        (bc.individual.percent_outside_campus_from_campus, scalar_type),  # v
        (bc.individual.time_at_campus, scalar_type),  # v
        (bc.individual.number_of_contacts_less, scalar_type),  # v
        (bc.individual.first_seen_response_rate, scalar_type),  # v
        (bc.individual.ratio_call_text, scalar_type),  # v
        (bc.individual.interaction_autocorrelation, scalar_type)  # v
    ]

    spatial_functions = [
        #(bc.spatial.number_of_antennas, scalar_type),
        #(bc.spatial.entropy_of_antennas, scalar_type),
        (bc.spatial.percent_at_home, scalar_type),
        (bc.spatial.radius_of_gyration, scalar_type),
        #(bc.spatial.frequent_antennas, scalar_type),
        (bc.spatial.churn_rate, scalar_type)
    ]

    network_functions = [
        bc.network.clustering_coefficient_unweighted,
        bc.network.clustering_coefficient_weighted,
        bc.network.assortativity_attributes#,
        #bc.network.assortativity_indicators
    ]

    interaction_types = [k for k,v in user.supported_types.iteritems() if v]
    groups = [
        [r for r in g] 
        for g in group_records(user, interaction_types, groupby)
    ]

    reporting = OrderedDict([
        ('attributes_path', user.attributes_path),
        ('version', bc.__version__),
        ('groupby', groupby),
        ('split_week', split_week),
        ('split_day', split_day),
        ('start_time_call', user.start_time['call'] and str(user.start_time['call'])),
        ('end_time_call', user.end_time['call'] and str(user.end_time['call'])),
        ('night_start', str(user.night_start)),
        ('night_end', str(user.night_end)),
        ('weekend', user.weekend),
        ('bins', len(groups)),
        ('has_call', user.supported_types['call']),
        ('has_text', user.supported_types['text']),
        ('has_physical', user.supported_types['physical']),
        ('has_screen', user.supported_types['screen']),
        ('has_stop', user.supported_types['stop']),
        ('has_home', user.has_home),
        ('has_network', user.has_network),
        ('percent_outofnetwork_calls', user.percent_outofnetwork_calls),
        ('percent_outofnetwork_texts', user.percent_outofnetwork_texts),
        ('percent_outofnetwork_contacts', user.percent_outofnetwork_contacts),
        ('percent_outofnetwork_call_durations', user.percent_outofnetwork_call_durations),
    ])

    record_interaction_types = [
        (user.call_records, user.ignored_records['call'], "call_records"),
        (user.text_records, user.ignored_records['text'], "text_records"),
        (user.physical_records, user.ignored_records['physical'], "physical_records"),
        (user.screen_records, user.ignored_records['screen'], "screen_records"),
        (user.stop_records, user.ignored_records['stop'], "stop_records"),
    ]

    for records, ignored_records, name in record_interaction_types:
        if records is not None:
            reporting['number_of_%s' % name] = len(records)
        else:
            reporting['number_of_%s' % name] = 0

        if ignored_records is not None:
            reporting['ignored_%s' % name] = ignored_records

    returned = OrderedDict([
        ('name', user.name),
        ('reporting', reporting)
    ])

    if spatial:
        functions = individual_functions + spatial_functions
    else:
        functions = individual_functions

    fun_times = dict()
    for fun, datatype in functions:
        start = dt.now()
        try:
            metric = fun(
                user, groupby=groupby, summary=summary, datatype=datatype,
                split_week=split_week, split_day=split_day
            )
        except ValueError:
            metric = fun(
                user, groupby=groupby, datatype=datatype,
                split_week=split_week, split_day=split_day
            )
            
        if len(metric) < 1:
            continue
        
        indicator_time = (dt.now() - start).microseconds * 1e-6

        fun_times[fun.__name__] = indicator_time
        returned[fun.__name__] = metric
    
    for n, t in sorted(fun_times.items(), key=lambda x: x[1])[-5:]:
        print warning_str("%s is slow - time: %.2f" % (n, t))

    if network and user.has_network:
        for fun in network_functions:
            returned[fun.__name__] = fun(user)

    if attributes and user.attributes != {}:
        returned['attributes'] = user.attributes

    if flatten is True:
        return globals()['flatten'](returned)

    return returned
