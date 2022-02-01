import json
from typing import List, Tuple
from unittest.mock import patch
from uuid import uuid4

from django.utils import timezone
from freezegun import freeze_time
from rest_framework.exceptions import ValidationError

from ee.clickhouse.models.event import create_event
from ee.clickhouse.models.person import create_person_distinct_id
from ee.clickhouse.util import ClickhouseTestMixin
from posthog.constants import TREND_FILTER_TYPE_EVENTS, TRENDS_BAR_VALUE, TRENDS_TABLE
from posthog.models import Action, ActionStep, Cohort, Entity, Filter, Organization, Person
from posthog.queries.trends.clickhouse_trends import ClickhouseTrends
from posthog.queries.trends.person import ClickhouseTrendsActors
from posthog.queries.trends.util import breakdown_label
from posthog.test.base import APIBaseTest, test_with_materialized_columns


def _create_action(**kwargs):
    team = kwargs.pop("team")
    name = kwargs.pop("name")
    properties = kwargs.pop("properties", {})
    action = Action.objects.create(team=team, name=name)
    ActionStep.objects.create(action=action, event=name, properties=properties)
    return action


def _create_cohort(**kwargs):
    team = kwargs.pop("team")
    name = kwargs.pop("name")
    groups = kwargs.pop("groups")
    cohort = Cohort.objects.create(team=team, name=name, groups=groups, last_calculation=timezone.now())
    return cohort


def _create_event(**kwargs):
    kwargs.update({"event_uuid": uuid4()})
    create_event(**kwargs)


class TestTrends(ClickhouseTestMixin, APIBaseTest):
    maxDiff = None

    def _get_trend_people(self, filter, entity):
        _, serialized_actors = ClickhouseTrendsActors(filter=filter, entity=entity, team=self.team).get_actors()
        return serialized_actors

    def _create_events(self, use_time=False) -> Tuple[Action, Person]:

        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        _, _, secondTeam = Organization.objects.bootstrap(None, team_fields={"api_token": "token456"})

        freeze_without_time = ["2019-12-24", "2020-01-01", "2020-01-02"]
        freeze_with_time = [
            "2019-12-24 03:45:34",
            "2020-01-01 00:06:34",
            "2020-01-02 16:34:34",
        ]

        freeze_args = freeze_without_time
        if use_time:
            freeze_args = freeze_with_time

        with freeze_time(freeze_args[0]):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$bool_prop": True},
            )

        with freeze_time(freeze_args[1]):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$bool_prop": False},
            )
            _create_event(team=self.team, event="sign up", distinct_id="anonymous_id", properties={"$bool_prop": False})
            _create_event(team=self.team, event="sign up", distinct_id="blabla")
        with freeze_time(freeze_args[2]):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "other_value", "$some_numerical_prop": 80,},
            )
            _create_event(team=self.team, event="no events", distinct_id="blabla")

            # second team should have no effect
            _create_event(
                team=secondTeam, event="sign up", distinct_id="blabla", properties={"$some_property": "other_value"},
            )

        no_events = _create_action(team=self.team, name="no events")
        sign_up_action = _create_action(team=self.team, name="sign up")

        return sign_up_action, person

    def _create_breakdown_events(self):
        freeze_without_time = ["2020-01-02"]

        with freeze_time(freeze_without_time[0]):
            for i in range(25):
                _create_event(
                    team=self.team, event="sign up", distinct_id="blabla", properties={"$some_property": i},
                )
        sign_up_action = _create_action(team=self.team, name="sign up")

    def test_trends_per_day(self):
        self._create_events()
        with freeze_time("2020-01-04T13:00:01Z"):
            # with self.assertNumQueries(16):
            response = ClickhouseTrends().run(
                Filter(data={"date_from": "-7d", "events": [{"id": "sign up"}, {"id": "no events"}],}), self.team,
            )
        self.assertEqual(response[0]["label"], "sign up")
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 3.0)
        self.assertEqual(response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[0]["data"][5], 1.0)

    # just make sure this doesn't error
    def test_no_props(self):
        with freeze_time("2020-01-04T13:01:01Z"):
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "$some_property",
                        "events": [
                            {"id": "sign up", "name": "sign up", "type": "events", "order": 0,},
                            {"id": "no events"},
                        ],
                    }
                ),
                self.team,
            )

    def test_trends_per_day_48hours(self):
        self._create_events()
        with freeze_time("2020-01-03T13:00:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={"date_from": "-48h", "interval": "day", "events": [{"id": "sign up"}, {"id": "no events"}],}
                ),
                self.team,
            )

        self.assertEqual(response[0]["data"][1], 1.0)
        self.assertEqual(response[0]["labels"][1], "2-Jan-2020")

    def test_trends_per_day_cumulative(self):
        self._create_events()
        with freeze_time("2020-01-04T13:00:01Z"):

            response = ClickhouseTrends().run(
                Filter(
                    data={"date_from": "-7d", "display": "ActionsLineGraphCumulative", "events": [{"id": "sign up"}],}
                ),
                self.team,
            )

        self.assertEqual(response[0]["label"], "sign up")
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 3.0)
        self.assertEqual(response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[0]["data"][5], 4.0)

    def test_trends_single_aggregate_dau(self):
        self._create_events()
        with freeze_time("2020-01-04T13:00:01Z"):
            daily_response = ClickhouseTrends().run(
                Filter(
                    data={"display": TRENDS_TABLE, "interval": "week", "events": [{"id": "sign up", "math": "dau"}],}
                ),
                self.team,
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            weekly_response = ClickhouseTrends().run(
                Filter(
                    data={"display": TRENDS_TABLE, "interval": "day", "events": [{"id": "sign up", "math": "dau"}],}
                ),
                self.team,
            )

        self.assertEqual(daily_response[0]["aggregated_value"], 1)
        self.assertEqual(daily_response[0]["aggregated_value"], weekly_response[0]["aggregated_value"])

    @test_with_materialized_columns(["$math_prop"])
    def test_trends_single_aggregate_math(self):
        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        with freeze_time("2020-01-01 00:06:34"):
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 1},
            )
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 1},
            )
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 1},
            )
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 2},
            )
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 3},
            )

        with freeze_time("2020-01-02 00:06:34"):
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 4},
            )
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$math_prop": 4},
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            daily_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "interval": "week",
                        "events": [{"id": "sign up", "math": "median", "math_property": "$math_prop"}],
                    }
                ),
                self.team,
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            weekly_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "interval": "day",
                        "events": [{"id": "sign up", "math": "median", "math_property": "$math_prop"}],
                    }
                ),
                self.team,
            )

        self.assertEqual(daily_response[0]["aggregated_value"], 2.0)
        self.assertEqual(daily_response[0]["aggregated_value"], weekly_response[0]["aggregated_value"])

    @test_with_materialized_columns(person_properties=["name"], verify_no_jsonextract=False)
    def test_trends_breakdown_single_aggregate_cohorts(self):
        person_1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["Jane"], properties={"name": "Jane"})
        person_2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["John"], properties={"name": "John"})
        person_3 = Person.objects.create(team_id=self.team.pk, distinct_ids=["Jill"], properties={"name": "Jill"})
        cohort1 = _create_cohort(
            team=self.team,
            name="cohort1",
            groups=[{"properties": [{"key": "name", "value": "Jane", "type": "person"}]}],
        )
        cohort2 = _create_cohort(
            team=self.team,
            name="cohort2",
            groups=[{"properties": [{"key": "name", "value": "John", "type": "person"}]}],
        )
        cohort3 = _create_cohort(
            team=self.team,
            name="cohort3",
            groups=[{"properties": [{"key": "name", "value": "Jill", "type": "person"}]}],
        )
        with freeze_time("2020-01-01 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="John",
                properties={"$some_property": "value", "$browser": "Chrome"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="John",
                properties={"$some_property": "value", "$browser": "Chrome"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="Jill",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="Jill",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="Jill",
                properties={"$some_property": "value", "$browser": "Safari"},
            )

        with freeze_time("2020-01-02 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="Jane",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="Jane",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
        with freeze_time("2020-01-04T13:00:01Z"):
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "breakdown": json.dumps([cohort1.pk, cohort2.pk, cohort3.pk, "all"]),
                        "breakdown_type": "cohort",
                        "events": [{"id": "sign up"}],
                    }
                ),
                self.team,
            )

        for result in event_response:
            if result["label"] == "sign up - cohort1":
                self.assertEqual(result["aggregated_value"], 2)
            elif result["label"] == "sign up - cohort2":
                self.assertEqual(result["aggregated_value"], 2)
            elif result["label"] == "sign up - cohort3":
                self.assertEqual(result["aggregated_value"], 3)
            else:
                self.assertEqual(result["aggregated_value"], 7)

    def test_trends_breakdown_single_aggregate(self):
        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        with freeze_time("2020-01-01 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Chrome"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Chrome"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Safari"},
            )

        with freeze_time("2020-01-02 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Safari"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$browser": "Safari"},
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            daily_response = ClickhouseTrends().run(
                Filter(data={"display": TRENDS_TABLE, "breakdown": "$browser", "events": [{"id": "sign up"}],}),
                self.team,
            )

        for result in daily_response:
            if result["breakdown_value"] == "Chrome":
                self.assertEqual(result["aggregated_value"], 2)
            else:
                self.assertEqual(result["aggregated_value"], 5)

    def test_trends_breakdown_single_aggregate_math(self):
        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        with freeze_time("2020-01-01 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 1},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 1},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 1},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 2},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 3},
            )

        with freeze_time("2020-01-02 00:06:34"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 4},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "value", "$math_prop": 4},
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            daily_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "interval": "day",
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "math": "median", "math_property": "$math_prop"}],
                    }
                ),
                self.team,
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            weekly_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "interval": "week",
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "math": "median", "math_property": "$math_prop"}],
                    }
                ),
                self.team,
            )

        self.assertEqual(daily_response[0]["aggregated_value"], 2.0)
        self.assertEqual(daily_response[0]["aggregated_value"], weekly_response[0]["aggregated_value"])

    @test_with_materialized_columns(["$math_prop", "$some_property"])
    def test_trends_breakdown_with_math_func(self):

        with freeze_time("2020-01-01 00:06:34"):
            for i in range(20):
                person = Person.objects.create(team_id=self.team.pk, distinct_ids=[f"person{i}"])
                _create_event(
                    team=self.team,
                    event="sign up",
                    distinct_id=f"person{i}",
                    properties={"$some_property": f"value_{i}", "$math_prop": 1},
                )
                _create_event(
                    team=self.team,
                    event="sign up",
                    distinct_id=f"person{i}",
                    properties={"$some_property": f"value_{i}", "$math_prop": 1},
                )

            person = Person.objects.create(team_id=self.team.pk, distinct_ids=[f"person21"])
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id=f"person21",
                properties={"$some_property": "value_21", "$math_prop": 25},
            )

        with freeze_time("2020-01-04T13:00:01Z"):
            daily_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "display": TRENDS_TABLE,
                        "interval": "day",
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "math": "p90", "math_property": "$math_prop"}],
                    }
                ),
                self.team,
            )

        breakdown_vals = [val["breakdown_value"] for val in daily_response]
        self.assertTrue("value_21" in breakdown_vals)

    def test_trends_compare(self):
        self._create_events()
        with freeze_time("2020-01-04T13:00:01Z"):
            response = ClickhouseTrends().run(
                Filter(data={"compare": "true", "events": [{"id": "sign up"}]}), self.team
            )

        self.assertEqual(response[0]["label"], "sign up")
        self.assertEqual(response[0]["labels"][4], "day 4")
        self.assertEqual(response[0]["data"][4], 3.0)
        self.assertEqual(response[0]["labels"][5], "day 5")
        self.assertEqual(response[0]["data"][5], 1.0)

        self.assertEqual(response[1]["label"], "sign up")
        self.assertEqual(response[1]["labels"][4], "day 4")
        self.assertEqual(response[1]["data"][4], 1.0)
        self.assertEqual(response[1]["labels"][5], "day 5")
        self.assertEqual(response[1]["data"][5], 0.0)

        with freeze_time("2020-01-04T13:00:01Z"):
            no_compare_response = ClickhouseTrends().run(
                Filter(data={"compare": "false", "events": [{"id": "sign up"}]}), self.team
            )

        self.assertEqual(no_compare_response[0]["label"], "sign up")
        self.assertEqual(no_compare_response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(no_compare_response[0]["data"][4], 3.0)
        self.assertEqual(no_compare_response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(no_compare_response[0]["data"][5], 1.0)

    def _test_events_with_dates(self, dates: List[str], result, query_time=None, **filter_params):
        person1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person_1"], properties={"name": "John"})
        for time in dates:
            with freeze_time(time):
                _create_event(
                    event="event_name", team=self.team, distinct_id="person_1", properties={"$browser": "Safari"},
                )

        if query_time:
            with freeze_time(query_time):
                response = ClickhouseTrends().run(
                    Filter(data={**filter_params, "events": [{"id": "event_name"}]}), self.team,
                )
        else:
            response = ClickhouseTrends().run(
                Filter(data={**filter_params, "events": [{"id": "event_name"}]}), self.team,
            )

        self.assertEqual(result[0]["count"], response[0]["count"])
        self.assertEqual(result[0]["data"], response[0]["data"])
        self.assertEqual(result[0]["labels"], response[0]["labels"])
        self.assertEqual(result[0]["days"], response[0]["days"])

    def test_hour_interval(self):
        self._test_events_with_dates(
            dates=["2020-11-01 13:00:00", "2020-11-01 13:20:00", "2020-11-01 17:00:00"],
            interval="hour",
            date_from="2020-11-01 12:00:00",
            date_to="2020-11-01 18:00:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3.0,
                    "data": [0.0, 2.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                    "labels": [
                        "1-Nov-2020 12:00",
                        "1-Nov-2020 13:00",
                        "1-Nov-2020 14:00",
                        "1-Nov-2020 15:00",
                        "1-Nov-2020 16:00",
                        "1-Nov-2020 17:00",
                        "1-Nov-2020 18:00",
                    ],
                    "days": [
                        "2020-11-01 12:00:00",
                        "2020-11-01 13:00:00",
                        "2020-11-01 14:00:00",
                        "2020-11-01 15:00:00",
                        "2020-11-01 16:00:00",
                        "2020-11-01 17:00:00",
                        "2020-11-01 18:00:00",
                    ],
                }
            ],
        )

    def test_day_interval(self):
        self._test_events_with_dates(
            dates=["2020-11-01", "2020-11-02", "2020-11-03", "2020-11-04"],
            interval="day",
            date_from="2020-11-01",
            date_to="2020-11-07",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 4.0,
                    "data": [1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0],
                    "labels": [
                        "1-Nov-2020",
                        "2-Nov-2020",
                        "3-Nov-2020",
                        "4-Nov-2020",
                        "5-Nov-2020",
                        "6-Nov-2020",
                        "7-Nov-2020",
                    ],
                    "days": [
                        "2020-11-01",
                        "2020-11-02",
                        "2020-11-03",
                        "2020-11-04",
                        "2020-11-05",
                        "2020-11-06",
                        "2020-11-07",
                    ],
                }
            ],
        )

    def test_week_interval(self):
        self._test_events_with_dates(
            dates=["2020-11-01", "2020-11-10", "2020-11-11", "2020-11-18"],
            interval="week",
            date_from="2020-10-29",  # having date after sunday + no events caused an issue in CH
            date_to="2020-11-24",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 4.0,
                    "data": [0.0, 1.0, 2.0, 1.0, 0.0],
                    "labels": ["25-Oct-2020", "1-Nov-2020", "8-Nov-2020", "15-Nov-2020", "22-Nov-2020",],
                    "days": ["2020-10-25", "2020-11-01", "2020-11-08", "2020-11-15", "2020-11-22"],
                }
            ],
        )

    def test_month_interval(self):
        self._test_events_with_dates(
            dates=["2020-07-10", "2020-07-30", "2020-10-18"],
            interval="month",
            date_from="2020-6-01",
            date_to="2020-11-24",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3.0,
                    "data": [0.0, 2.0, 0.0, 0.0, 1.0, 0.0],
                    "labels": ["1-Jun-2020", "1-Jul-2020", "1-Aug-2020", "1-Sep-2020", "1-Oct-2020", "1-Nov-2020"],
                    "days": ["2020-06-01", "2020-07-01", "2020-08-01", "2020-09-01", "2020-10-01", "2020-11-01"],
                }
            ],
        )

    def test_interval_rounding(self):
        self._test_events_with_dates(
            dates=["2020-11-01", "2020-11-10", "2020-11-11", "2020-11-18"],
            interval="week",
            date_from="2020-11-04",
            date_to="2020-11-24",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 4.0,
                    "data": [1.0, 2.0, 1.0, 0.0],
                    "labels": ["1-Nov-2020", "8-Nov-2020", "15-Nov-2020", "22-Nov-2020"],
                    "days": ["2020-11-01", "2020-11-08", "2020-11-15", "2020-11-22"],
                }
            ],
        )

    def test_interval_rounding_monthly(self):
        self._test_events_with_dates(
            dates=["2020-06-2", "2020-07-30",],
            interval="month",
            date_from="2020-6-7",  #  should round down to 6-1
            date_to="2020-7-30",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 2.0,
                    "data": [1.0, 1.0,],
                    "labels": ["1-Jun-2020", "1-Jul-2020"],
                    "days": ["2020-06-01", "2020-07-01",],
                }
            ],
        )

    def test_today_timerange(self):
        self._test_events_with_dates(
            dates=["2020-11-01 10:20:00", "2020-11-01 10:22:00", "2020-11-01 10:25:00"],
            date_from="dStart",
            query_time="2020-11-01 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3,
                    "data": [3],
                    "labels": ["1-Nov-2020"],
                    "days": ["2020-11-01"],
                }
            ],
        )

    def test_yesterday_timerange(self):
        self._test_events_with_dates(
            dates=["2020-11-01 05:20:00", "2020-11-01 10:22:00", "2020-11-01 10:25:00"],
            date_from="-1d",
            date_to="-1d",
            query_time="2020-11-02 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3.0,
                    "data": [3.0],
                    "labels": ["1-Nov-2020"],
                    "days": ["2020-11-01"],
                }
            ],
        )

    def test_last24hours_timerange(self):
        self._test_events_with_dates(
            dates=["2020-11-01 05:20:00", "2020-11-01 10:22:00", "2020-11-01 10:25:00", "2020-11-02 08:25:00"],
            date_from="-24h",
            query_time="2020-11-02 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3,
                    "data": [2, 1],
                    "labels": ["1-Nov-2020", "2-Nov-2020"],
                    "days": ["2020-11-01", "2020-11-02"],
                }
            ],
        )

    def test_last48hours_timerange(self):
        self._test_events_with_dates(
            dates=["2020-11-01 05:20:00", "2020-11-01 10:22:00", "2020-11-01 10:25:00", "2020-11-02 08:25:00"],
            date_from="-48h",
            query_time="2020-11-02 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 4.0,
                    "data": [0.0, 3.0, 1.0],
                    "labels": ["31-Oct-2020", "1-Nov-2020", "2-Nov-2020"],
                    "days": ["2020-10-31", "2020-11-01", "2020-11-02"],
                }
            ],
        )

    def test_last7days_timerange(self):
        self._test_events_with_dates(
            dates=["2020-11-01 05:20:00", "2020-11-02 10:22:00", "2020-11-04 10:25:00", "2020-11-05 08:25:00"],
            date_from="-7d",
            query_time="2020-11-07 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 4.0,
                    "data": [0.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0],
                    "labels": [
                        "31-Oct-2020",
                        "1-Nov-2020",
                        "2-Nov-2020",
                        "3-Nov-2020",
                        "4-Nov-2020",
                        "5-Nov-2020",
                        "6-Nov-2020",
                        "7-Nov-2020",
                    ],
                    "days": [
                        "2020-10-31",
                        "2020-11-01",
                        "2020-11-02",
                        "2020-11-03",
                        "2020-11-04",
                        "2020-11-05",
                        "2020-11-06",
                        "2020-11-07",
                    ],
                }
            ],
        )

    def test_last14days_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-11-01 05:20:00",
                "2020-11-02 10:22:00",
                "2020-11-04 10:25:00",
                "2020-11-05 08:25:00",
                "2020-11-05 08:25:00",
                "2020-11-10 08:25:00",
            ],
            date_from="-14d",
            query_time="2020-11-14 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 6.0,
                    "data": [0.0, 1.0, 1.0, 0.0, 1.0, 2.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
                    "labels": [
                        "31-Oct-2020",
                        "1-Nov-2020",
                        "2-Nov-2020",
                        "3-Nov-2020",
                        "4-Nov-2020",
                        "5-Nov-2020",
                        "6-Nov-2020",
                        "7-Nov-2020",
                        "8-Nov-2020",
                        "9-Nov-2020",
                        "10-Nov-2020",
                        "11-Nov-2020",
                        "12-Nov-2020",
                        "13-Nov-2020",
                        "14-Nov-2020",
                    ],
                    "days": [
                        "2020-10-31",
                        "2020-11-01",
                        "2020-11-02",
                        "2020-11-03",
                        "2020-11-04",
                        "2020-11-05",
                        "2020-11-06",
                        "2020-11-07",
                        "2020-11-08",
                        "2020-11-09",
                        "2020-11-10",
                        "2020-11-11",
                        "2020-11-12",
                        "2020-11-13",
                        "2020-11-14",
                    ],
                }
            ],
        )

    def test_last30days_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-11-01 05:20:00",
                "2020-11-11 10:22:00",
                "2020-11-24 10:25:00",
                "2020-11-05 08:25:00",
                "2020-11-05 08:25:00",
                "2020-11-10 08:25:00",
            ],
            date_from="-30d",
            interval="week",
            query_time="2020-11-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 6.0,
                    "data": [0.0, 3.0, 2.0, 0.0, 1.0, 0.0],
                    "labels": ["25-Oct-2020", "1-Nov-2020", "8-Nov-2020", "15-Nov-2020", "22-Nov-2020", "29-Nov-2020",],
                    "days": ["2020-10-25", "2020-11-01", "2020-11-08", "2020-11-15", "2020-11-22", "2020-11-29"],
                }
            ],
        )

    def test_last90days_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-09-01 05:20:00",
                "2020-10-05 05:20:00",
                "2020-10-20 05:20:00",
                "2020-11-01 05:20:00",
                "2020-11-11 10:22:00",
                "2020-11-24 10:25:00",
                "2020-11-05 08:25:00",
                "2020-11-05 08:25:00",
                "2020-11-10 08:25:00",
            ],
            date_from="-90d",
            interval="month",
            query_time="2020-11-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 9,
                    "data": [1, 2, 6],
                    "labels": ["1-Sep-2020", "1-Oct-2020", "1-Nov-2020"],
                    "days": ["2020-09-01", "2020-10-01", "2020-11-01"],
                }
            ],
        )

    def test_this_month_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-11-01 05:20:00",
                "2020-11-11 10:22:00",
                "2020-11-24 10:25:00",
                "2020-11-05 08:25:00",
                "2020-11-05 08:25:00",
                "2020-11-10 08:25:00",
            ],
            date_from="mStart",
            interval="month",
            query_time="2020-11-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 6,
                    "data": [6],
                    "labels": ["1-Nov-2020"],
                    "days": ["2020-11-01"],
                }
            ],
        )

    def test_previous_month_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-11-01 05:20:00",
                "2020-11-11 10:22:00",
                "2020-11-24 10:25:00",
                "2020-11-05 08:25:00",
                "2020-11-05 08:25:00",
                "2020-11-10 08:25:00",
            ],
            date_from="-1mStart",
            date_to="-1mEnd",
            interval="month",
            query_time="2020-12-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 6,
                    "data": [6],
                    "labels": ["1-Nov-2020"],
                    "days": ["2020-11-01"],
                }
            ],
        )

    def test_year_to_date_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-01-01 05:20:00",
                "2020-01-11 10:22:00",
                "2020-02-24 10:25:00",
                "2020-02-05 08:25:00",
                "2020-03-05 08:25:00",
                "2020-05-10 08:25:00",
            ],
            date_from="yStart",
            interval="month",
            query_time="2020-04-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 5.0,
                    "data": [2.0, 2.0, 1.0, 0.0],
                    "labels": ["1-Jan-2020", "1-Feb-2020", "1-Mar-2020", "1-Apr-2020"],
                    "days": ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"],
                }
            ],
        )

    def test_all_time_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-01-01 05:20:00",
                "2020-01-11 10:22:00",
                "2020-02-24 10:25:00",
                "2020-02-05 08:25:00",
                "2020-03-05 08:25:00",
            ],
            date_from="all",
            interval="month",
            query_time="2020-04-30 10:20:00",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 5.0,
                    "data": [2.0, 2.0, 1.0, 0.0],
                    "labels": ["1-Jan-2020", "1-Feb-2020", "1-Mar-2020", "1-Apr-2020"],
                    "days": ["2020-01-01", "2020-02-01", "2020-03-01", "2020-04-01"],
                }
            ],
        )

    def test_custom_range_timerange(self):
        self._test_events_with_dates(
            dates=[
                "2020-01-05 05:20:00",
                "2020-01-05 10:22:00",
                "2020-01-04 10:25:00",
                "2020-01-11 08:25:00",
                "2020-01-09 08:25:00",
            ],
            date_from="2020-01-05",
            query_time="2020-01-10",
            result=[
                {
                    "action": {
                        "id": "event_name",
                        "type": "events",
                        "order": None,
                        "name": "event_name",
                        "custom_name": None,
                        "math": None,
                        "math_property": None,
                        "math_group_type_index": None,
                        "properties": [],
                    },
                    "label": "event_name",
                    "count": 3.0,
                    "data": [2.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                    "labels": ["5-Jan-2020", "6-Jan-2020", "7-Jan-2020", "8-Jan-2020", "9-Jan-2020", "10-Jan-2020"],
                    "days": ["2020-01-05", "2020-01-06", "2020-01-07", "2020-01-08", "2020-01-09", "2020-01-10"],
                }
            ],
        )

    @test_with_materialized_columns(["$some_property"])
    def test_property_filtering(self):
        self._create_events()
        with freeze_time("2020-01-04"):
            response = ClickhouseTrends().run(
                Filter(
                    data={"properties": [{"key": "$some_property", "value": "value"}], "events": [{"id": "sign up"}],}
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 1.0)
        self.assertEqual(response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[0]["data"][5], 0)

    @test_with_materialized_columns(person_properties=["name"])
    def test_filter_events_by_cohort(self):
        person1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person_1"], properties={"name": "John"})
        person2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person_2"], properties={"name": "Jane"})

        event1 = _create_event(
            event="event_name", team=self.team, distinct_id="person_1", properties={"$browser": "Safari"},
        )
        event2 = _create_event(
            event="event_name", team=self.team, distinct_id="person_2", properties={"$browser": "Chrome"},
        )
        event3 = _create_event(
            event="event_name", team=self.team, distinct_id="person_2", properties={"$browser": "Safari"},
        )

        cohort = _create_cohort(
            team=self.team,
            name="cohort1",
            groups=[{"properties": [{"key": "name", "value": "Jane", "type": "person"}]}],
        )

        response = ClickhouseTrends().run(
            Filter(
                data={
                    "properties": [{"key": "id", "value": cohort.pk, "type": "cohort"}],
                    "events": [{"id": "event_name"}],
                },
                team=self.team,
            ),
            self.team,
        )

        self.assertEqual(response[0]["count"], 2)
        self.assertEqual(response[0]["data"][-1], 2)

    def test_response_empty_if_no_events(self):
        self._create_events()
        response = ClickhouseTrends().run(Filter(data={"date_from": "2012-12-12"}), self.team)
        self.assertEqual(response, [])

    def test_interval_filtering(self):
        self._create_events(use_time=True)

        # test hour
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(data={"date_from": "2019-12-24", "interval": "hour", "events": [{"id": "sign up"}]}), self.team,
            )
        self.assertEqual(response[0]["labels"][3], "24-Dec-2019 03:00")
        self.assertEqual(response[0]["data"][3], 1.0)
        # 217 - 24 - 1
        self.assertEqual(response[0]["data"][192], 3.0)

        # test week
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        #  2019-11-24 is a Sunday, i.e. beginning of our week
                        "date_from": "2019-11-24",
                        "interval": "week",
                        "events": [{"id": "sign up"}],
                    }
                ),
                self.team,
            )
        self.assertEqual(
            response[0]["labels"][:5], ["24-Nov-2019", "1-Dec-2019", "8-Dec-2019", "15-Dec-2019", "22-Dec-2019"]
        )
        self.assertEqual(response[0]["data"][:5], [0.0, 0.0, 0.0, 0.0, 1.0])

        # test month
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(data={"date_from": "2019-9-24", "interval": "month", "events": [{"id": "sign up"}]}), self.team,
            )
        self.assertEqual(response[0]["labels"][0], "1-Sep-2019")
        self.assertEqual(response[0]["data"][0], 0)
        self.assertEqual(response[0]["labels"][3], "1-Dec-2019")
        self.assertEqual(response[0]["data"][3], 1.0)
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 4.0)

        with freeze_time("2020-01-02 23:30"):
            _create_event(team=self.team, event="sign up", distinct_id="blabla")

        # test today + hourly
        with freeze_time("2020-01-02T23:31:00Z"):
            response = ClickhouseTrends().run(
                Filter(data={"date_from": "dStart", "interval": "hour", "events": [{"id": "sign up"}]}), self.team
            )
        self.assertEqual(response[0]["labels"][23], "2-Jan-2020 23:00")
        self.assertEqual(response[0]["data"][23], 1.0)

    def test_breakdown_label(self):
        entity = Entity({"id": "$pageview", "name": "$pageview", "type": TREND_FILTER_TYPE_EVENTS})
        num_label = breakdown_label(entity, 1)
        self.assertEqual(num_label, {"label": "$pageview - 1", "breakdown_value": 1})

        string_label = breakdown_label(entity, "Chrome")
        self.assertEqual(string_label, {"label": "$pageview - Chrome", "breakdown_value": "Chrome"})

        nan_label = breakdown_label(entity, "nan")
        self.assertEqual(nan_label, {"label": "$pageview - Other", "breakdown_value": "Other"})

        none_label = breakdown_label(entity, "None")
        self.assertEqual(none_label, {"label": "$pageview - Other", "breakdown_value": "Other"})

        cohort_all_label = breakdown_label(entity, "cohort_all")
        self.assertEqual(cohort_all_label, {"label": "$pageview - all users", "breakdown_value": "all"})

        cohort = _create_cohort(team=self.team, name="cohort1", groups=[{"properties": {"name": "Jane"}}])
        cohort_label = breakdown_label(entity, f"cohort_{cohort.pk}")
        self.assertEqual(cohort_label, {"label": f"$pageview - {cohort.name}", "breakdown_value": cohort.pk})

    @test_with_materialized_columns(["$some_property"])
    def test_breakdown_filtering(self):
        self._create_events()
        # test breakdown filtering
        with freeze_time("2020-01-04T13:01:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "$some_property",
                        "events": [
                            {"id": "sign up", "name": "sign up", "type": "events", "order": 0,},
                            {"id": "no events"},
                        ],
                    }
                ),
                self.team,
            )

        self.assertEqual(response[0]["label"], "sign up - none")
        self.assertEqual(response[1]["label"], "sign up - value")
        self.assertEqual(response[2]["label"], "sign up - other_value")
        self.assertEqual(response[3]["label"], "no events - none")

        self.assertEqual(sum(response[0]["data"]), 2)
        self.assertEqual(sum(response[1]["data"]), 2)
        self.assertEqual(sum(response[2]["data"]), 1)
        self.assertEqual(sum(response[3]["data"]), 1)

    @test_with_materialized_columns(["$some_property"])
    def test_breakdown_filtering_limit(self):
        self._create_breakdown_events()
        with freeze_time("2020-01-04T13:01:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0}],
                    }
                ),
                self.team,
            )
        self.assertEqual(len(response), 25)  # We fetch 25 to see if there are more ethan 20 values

    @test_with_materialized_columns(event_properties=["key"], person_properties=["email"])
    def test_breakdown_user_props_with_filter(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"], properties={"email": "test@posthog.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"], properties={"email": "test@gmail.com"})
        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["person3"], properties={"email": "test@gmail.com"}
        )
        create_person_distinct_id(self.team.pk, "person1", str(person.uuid))

        _create_event(event="sign up", distinct_id="person1", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person2", team=self.team, properties={"key": "val"})
        response = ClickhouseTrends().run(
            Filter(
                data={
                    "date_from": "-14d",
                    "breakdown": "email",
                    "breakdown_type": "person",
                    "events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0,}],
                    "properties": [
                        {"key": "email", "value": "@posthog.com", "operator": "not_icontains", "type": "person"},
                        {"key": "key", "value": "val"},
                    ],
                }
            ),
            self.team,
        )

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["breakdown_value"], "test@gmail.com")

    @test_with_materialized_columns(["key"])
    def test_breakdown_with_filter(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"], properties={"email": "test@posthog.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"], properties={"email": "test@gmail.com"})
        _create_event(event="sign up", distinct_id="person1", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person2", team=self.team, properties={"key": "oh"})
        response = ClickhouseTrends().run(
            Filter(
                data={
                    "date_from": "-14d",
                    "breakdown": "key",
                    "events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0,}],
                    "properties": [{"key": "key", "value": "oh", "operator": "not_icontains"}],
                }
            ),
            self.team,
        )
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["breakdown_value"], "val")

    def test_action_filtering(self):
        sign_up_action, person = self._create_events()
        action_response = ClickhouseTrends().run(Filter(data={"actions": [{"id": sign_up_action.id}]}), self.team)
        event_response = ClickhouseTrends().run(Filter(data={"events": [{"id": "sign up"}]}), self.team)
        self.assertEqual(len(action_response), 1)

        self.assertEntityResponseEqual(action_response, event_response)

    def test_trends_for_non_existing_action(self):
        with freeze_time("2020-01-04"):
            response = ClickhouseTrends().run(Filter(data={"actions": [{"id": 50000000}]}), self.team)
        self.assertEqual(len(response), 0)

        with freeze_time("2020-01-04"):
            response = ClickhouseTrends().run(Filter(data={"events": [{"id": "DNE"}]}), self.team)
        self.assertEqual(response[0]["data"], [0, 0, 0, 0, 0, 0, 0, 0])

    @test_with_materialized_columns(person_properties=["email", "bar"])
    def test_trends_regression_filtering_by_action_with_person_properties(self):
        person1 = Person.objects.create(
            team_id=self.team.pk, properties={"email": "foo@example.com", "bar": "aa"}, distinct_ids=["d1"]
        )
        person2 = Person.objects.create(
            team_id=self.team.pk, properties={"email": "bar@example.com", "bar": "bb"}, distinct_ids=["d2"]
        )
        person2 = Person.objects.create(
            team_id=self.team.pk, properties={"email": "efg@example.com", "bar": "ab"}, distinct_ids=["d3"]
        )
        person3 = Person.objects.create(team_id=self.team.pk, properties={"bar": "aa"}, distinct_ids=["d4"])

        with freeze_time("2020-01-02 16:34:34"):
            _create_event(team=self.team, event="$pageview", distinct_id="d1")
            _create_event(team=self.team, event="$pageview", distinct_id="d2")
            _create_event(team=self.team, event="$pageview", distinct_id="d3")
            _create_event(team=self.team, event="$pageview", distinct_id="d4")

        event_filtering_action = Action.objects.create(team=self.team, name="$pageview from non-internal")
        ActionStep.objects.create(
            action=event_filtering_action,
            event="$pageview",
            properties=[{"key": "bar", "type": "person", "value": "a", "operator": "icontains"}],
        )
        event_filtering_action.calculate_events()

        with freeze_time("2020-01-04T13:01:01Z"):
            response = ClickhouseTrends().run(Filter({"actions": [{"id": event_filtering_action.id}],}), self.team)
        self.assertEqual(len(response), 1)
        self.assertEqual(response[0]["count"], 3)

        with freeze_time("2020-01-04T13:01:01Z"):
            response_with_email_filter = ClickhouseTrends().run(
                Filter(
                    {
                        "actions": [{"id": event_filtering_action.id}],
                        "properties": [{"key": "email", "type": "person", "value": "is_set", "operator": "is_set"}],
                    }
                ),
                self.team,
            )
        self.assertEqual(len(response_with_email_filter), 1)
        self.assertEqual(response_with_email_filter[0]["count"], 2)

    def test_dau_filtering(self):
        sign_up_action, person = self._create_events()

        with freeze_time("2020-01-02"):
            Person.objects.create(team_id=self.team.pk, distinct_ids=["someone_else"])
            _create_event(team=self.team, event="sign up", distinct_id="someone_else")

        sign_up_action.calculate_events()

        with freeze_time("2020-01-04"):
            action_response = ClickhouseTrends().run(
                Filter(data={"actions": [{"id": sign_up_action.id, "math": "dau"}]}), self.team
            )
            response = ClickhouseTrends().run(Filter(data={"events": [{"id": "sign up", "math": "dau"}]}), self.team)

        self.assertEqual(response[0]["data"][4], 1)
        self.assertEqual(response[0]["data"][5], 2)
        self.assertEntityResponseEqual(action_response, response)

    @test_with_materialized_columns(["$some_property"])
    def test_dau_with_breakdown_filtering(self):
        sign_up_action, _ = self._create_events()
        with freeze_time("2020-01-02T13:01:01Z"):
            _create_event(
                team=self.team, event="sign up", distinct_id="blabla", properties={"$some_property": "other_value"},
            )
        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(data={"breakdown": "$some_property", "actions": [{"id": sign_up_action.id, "math": "dau"}]}),
                self.team,
            )
            event_response = ClickhouseTrends().run(
                Filter(data={"breakdown": "$some_property", "events": [{"id": "sign up", "math": "dau"}]}), self.team,
            )

        self.assertEqual(event_response[1]["label"], "sign up - value")
        self.assertEqual(event_response[2]["label"], "sign up - other_value")

        self.assertEqual(sum(event_response[1]["data"]), 1)
        self.assertEqual(event_response[1]["data"][4], 1)  # property not defined

        self.assertEqual(sum(event_response[2]["data"]), 1)
        self.assertEqual(event_response[2]["data"][5], 1)
        self.assertEntityResponseEqual(action_response, event_response)

    def _create_maths_events(self, values):
        sign_up_action, person = self._create_events()
        Person.objects.create(team_id=self.team.pk, distinct_ids=["someone_else"])
        for value in values:
            _create_event(
                team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": value}
            )
        _create_event(team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": None})
        return sign_up_action

    def _test_math_property_aggregation(self, math_property, values, expected_value):
        sign_up_action = self._create_maths_events(values)

        action_response = ClickhouseTrends().run(
            Filter(
                data={"actions": [{"id": sign_up_action.id, "math": math_property, "math_property": "some_number"}]}
            ),
            self.team,
        )
        event_response = ClickhouseTrends().run(
            Filter(data={"events": [{"id": "sign up", "math": math_property, "math_property": "some_number"}]}),
            self.team,
        )
        # :TRICKY: Work around clickhouse functions not being 100%
        self.assertAlmostEqual(action_response[0]["data"][-1], expected_value, delta=0.5)
        self.assertEntityResponseEqual(action_response, event_response)

    @test_with_materialized_columns(["some_number"])
    def test_sum_filtering(self):
        self._test_math_property_aggregation("sum", values=[2, 3, 5.5, 7.5], expected_value=18)

    @test_with_materialized_columns(["some_number"])
    def test_avg_filtering(self):
        self._test_math_property_aggregation("avg", values=[2, 3, 5.5, 7.5], expected_value=4.5)

    @test_with_materialized_columns(["some_number"])
    def test_min_filtering(self):
        self._test_math_property_aggregation("min", values=[2, 3, 5.5, 7.5], expected_value=2)

    @test_with_materialized_columns(["some_number"])
    def test_max_filtering(self):
        self._test_math_property_aggregation("max", values=[2, 3, 5.5, 7.5], expected_value=7.5)

    @test_with_materialized_columns(["some_number"])
    def test_median_filtering(self):
        self._test_math_property_aggregation("median", values=range(101, 201), expected_value=150)

    @test_with_materialized_columns(["some_number"])
    def test_p90_filtering(self):
        self._test_math_property_aggregation("p90", values=range(101, 201), expected_value=190)

    @test_with_materialized_columns(["some_number"])
    def test_p95_filtering(self):
        self._test_math_property_aggregation("p95", values=range(101, 201), expected_value=195)

    @test_with_materialized_columns(["some_number"])
    def test_p99_filtering(self):
        self._test_math_property_aggregation("p99", values=range(101, 201), expected_value=199)

    @test_with_materialized_columns(["some_number"])
    def test_avg_filtering_non_number_resiliency(self):
        sign_up_action, person = self._create_events()
        Person.objects.create(team_id=self.team.pk, distinct_ids=["someone_else"])
        _create_event(team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": 2})
        _create_event(team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": "x"})
        _create_event(team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": None})
        _create_event(team=self.team, event="sign up", distinct_id="someone_else", properties={"some_number": 8})
        action_response = ClickhouseTrends().run(
            Filter(data={"actions": [{"id": sign_up_action.id, "math": "avg", "math_property": "some_number"}]}),
            self.team,
        )
        event_response = ClickhouseTrends().run(
            Filter(data={"events": [{"id": "sign up", "math": "avg", "math_property": "some_number"}]}), self.team
        )
        self.assertEqual(action_response[0]["data"][-1], 5)
        self.assertEntityResponseEqual(action_response, event_response)

    @test_with_materialized_columns(["$some_property"])
    def test_per_entity_filtering(self):
        self._create_events()
        with freeze_time("2020-01-04T13:00:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-7d",
                        "events": [
                            {"id": "sign up", "properties": [{"key": "$some_property", "value": "value"}],},
                            {"id": "sign up", "properties": [{"key": "$some_property", "value": "other_value"}],},
                        ],
                    }
                ),
                self.team,
            )

        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 1)
        self.assertEqual(response[0]["count"], 1)
        self.assertEqual(response[1]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[1]["data"][5], 1)
        self.assertEqual(response[1]["count"], 1)

    def _create_multiple_people(self):
        person1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"], properties={"name": "person1"})
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person1",
            timestamp="2020-01-01T12:00:00Z",
            properties={"order": "1"},
        )

        person2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"], properties={"name": "person2"})
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person2",
            timestamp="2020-01-01T12:00:00Z",
            properties={"order": "1"},
        )
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person2",
            timestamp="2020-01-02T12:00:00Z",
            properties={"order": "2"},
        )
        # same day
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person2",
            timestamp="2020-01-02T12:00:00Z",
            properties={"order": "2"},
        )

        person3 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person3"], properties={"name": "person3"})
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person3",
            timestamp="2020-01-01T12:00:00Z",
            properties={"order": "1"},
        )
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person3",
            timestamp="2020-01-02T12:00:00Z",
            properties={"order": "2"},
        )
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person3",
            timestamp="2020-01-03T12:00:00Z",
            properties={"order": "2"},
        )

        person4 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person4"], properties={"name": "person4"})
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person4",
            timestamp="2020-01-05T12:00:00Z",
            properties={"order": "1"},
        )

        return (person1, person2, person3, person4)

    @test_with_materialized_columns(person_properties=["name"])
    def test_person_property_filtering(self):
        self._create_multiple_people()
        with freeze_time("2020-01-04"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "properties": [{"key": "name", "value": "person1", "type": "person",}],
                        "events": [{"id": "watched movie"}],
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 1.0)
        self.assertEqual(response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[0]["data"][5], 0)

    @test_with_materialized_columns(person_properties=["name"])
    def test_entity_person_property_filtering(self):
        self._create_multiple_people()
        with freeze_time("2020-01-04"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "events": [
                            {
                                "id": "watched movie",
                                "properties": [{"key": "name", "value": "person1", "type": "person",}],
                            }
                        ],
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 1.0)
        self.assertEqual(response[0]["labels"][5], "2-Jan-2020")
        self.assertEqual(response[0]["data"][5], 0)

    def test_breakdown_by_empty_cohort(self):
        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"name": "p1"})
        _create_event(
            team=self.team, event="$pageview", distinct_id="p1", timestamp="2020-01-04T12:00:00Z",
        )

        with freeze_time("2020-01-04T13:01:01Z"):
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": json.dumps(["all"]),
                        "breakdown_type": "cohort",
                        "events": [{"id": "$pageview", "type": "events", "order": 0}],
                    }
                ),
                self.team,
            )

        self.assertEqual(event_response[0]["label"], "$pageview - all users")
        self.assertEqual(sum(event_response[0]["data"]), 1)

    @test_with_materialized_columns(person_properties=["name"], verify_no_jsonextract=False)
    def test_breakdown_by_cohort(self):
        person1, person2, person3, person4 = self._create_multiple_people()
        cohort = _create_cohort(name="cohort1", team=self.team, groups=[{"properties": {"name": "person1"}}])
        cohort2 = _create_cohort(name="cohort2", team=self.team, groups=[{"properties": {"name": "person2"}}])
        cohort3 = _create_cohort(
            name="cohort3",
            team=self.team,
            groups=[{"properties": {"name": "person1"}}, {"properties": {"name": "person2"}},],
        )
        action = _create_action(name="watched movie", team=self.team)
        action.calculate_events()

        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": json.dumps([cohort.pk, cohort2.pk, cohort3.pk, "all"]),
                        "breakdown_type": "cohort",
                        "actions": [{"id": action.pk, "type": "actions", "order": 0}],
                    }
                ),
                self.team,
            )
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": json.dumps([cohort.pk, cohort2.pk, cohort3.pk, "all"]),
                        "breakdown_type": "cohort",
                        "events": [{"id": "watched movie", "name": "watched movie", "type": "events", "order": 0,}],
                    }
                ),
                self.team,
            )

        counts = {}
        break_val = {}
        for res in event_response:
            counts[res["label"]] = sum(res["data"])
            break_val[res["label"]] = res["breakdown_value"]

        self.assertEqual(counts["watched movie - cohort1"], 1)
        self.assertEqual(counts["watched movie - cohort2"], 3)
        self.assertEqual(counts["watched movie - cohort3"], 4)
        self.assertEqual(counts["watched movie - all users"], 7)

        self.assertEqual(break_val["watched movie - cohort1"], cohort.pk)
        self.assertEqual(break_val["watched movie - cohort2"], cohort2.pk)
        self.assertEqual(break_val["watched movie - cohort3"], cohort3.pk)
        self.assertEqual(break_val["watched movie - all users"], "all")

        self.assertEntityResponseEqual(
            event_response, action_response,
        )

    @test_with_materialized_columns(verify_no_jsonextract=False)
    def test_interval_filtering_breakdown(self):
        self._create_events(use_time=True)
        cohort = _create_cohort(
            name="cohort1",
            team=self.team,
            groups=[{"properties": [{"key": "$some_prop", "value": "some_val", "type": "person"}]}],
        )

        # test hour
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "2019-12-24",
                        "interval": "hour",
                        "events": [{"id": "sign up"}],
                        "breakdown": json.dumps([cohort.pk]),
                        "breakdown_type": "cohort",
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][3], "24-Dec-2019 03:00")
        self.assertEqual(response[0]["data"][3], 1.0)
        # 217 - 24 - 1
        self.assertEqual(response[0]["data"][192], 3.0)

        # test week
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        # 2019-11-24 is a Sunday
                        "date_from": "2019-11-24",
                        "interval": "week",
                        "events": [{"id": "sign up"}],
                        "breakdown": json.dumps([cohort.pk]),
                        "breakdown_type": "cohort",
                    }
                ),
                self.team,
            )

        self.assertEqual(
            response[0]["labels"][:5], ["24-Nov-2019", "1-Dec-2019", "8-Dec-2019", "15-Dec-2019", "22-Dec-2019"]
        )
        self.assertEqual(response[0]["data"][:5], [0.0, 0.0, 0.0, 0.0, 1.0])

        # test month
        with freeze_time("2020-01-02"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "2019-9-24",
                        "interval": "month",
                        "events": [{"id": "sign up"}],
                        "breakdown": json.dumps([cohort.pk]),
                        "breakdown_type": "cohort",
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][3], "1-Dec-2019")
        self.assertEqual(response[0]["data"][3], 1.0)
        self.assertEqual(response[0]["labels"][4], "1-Jan-2020")
        self.assertEqual(response[0]["data"][4], 4.0)

        with freeze_time("2020-01-02 23:30"):
            _create_event(team=self.team, event="sign up", distinct_id="blabla")

        # test today + hourly
        with freeze_time("2020-01-02T23:31:00Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "dStart",
                        "interval": "hour",
                        "events": [{"id": "sign up"}],
                        "breakdown": json.dumps([cohort.pk]),
                        "breakdown_type": "cohort",
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["labels"][23], "2-Jan-2020 23:00")
        self.assertEqual(response[0]["data"][23], 1.0)

    def test_breakdown_by_person_property(self):
        person1, person2, person3, person4 = self._create_multiple_people()
        action = _create_action(name="watched movie", team=self.team)

        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "name",
                        "breakdown_type": "person",
                        "actions": [{"id": action.pk, "type": "actions", "order": 0}],
                    }
                ),
                self.team,
            )
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "name",
                        "breakdown_type": "person",
                        "events": [{"id": "watched movie", "name": "watched movie", "type": "events", "order": 0,}],
                    }
                ),
                self.team,
            )

        self.assertListEqual(
            sorted(res["breakdown_value"] for res in event_response), ["person1", "person2", "person3"]
        )

        for response in event_response:
            if response["breakdown_value"] == "person1":
                self.assertEqual(response["count"], 1)
                self.assertEqual(response["label"], "watched movie - person1")
            if response["breakdown_value"] == "person2":
                self.assertEqual(response["count"], 3)
            if response["breakdown_value"] == "person3":
                self.assertEqual(response["count"], 3)

        self.assertEntityResponseEqual(
            event_response, action_response,
        )

    def test_breakdown_by_property_pie(self):
        person1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"])
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person1",
            timestamp="2020-01-01T12:00:00Z",
            properties={"fake_prop": "value_1"},
        )

        person2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"])
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person2",
            timestamp="2020-01-01T12:00:00Z",
            properties={"fake_prop": "value_1"},
        )
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person2",
            timestamp="2020-01-02T12:00:00Z",
            properties={"fake_prop": "value_2"},
        )

        person3 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person3"])
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person3",
            timestamp="2020-01-01T12:00:00Z",
            properties={"fake_prop": "value_1"},
        )

        person4 = Person.objects.create(team_id=self.team.pk, distinct_ids=["person4"])
        _create_event(
            team=self.team,
            event="watched movie",
            distinct_id="person4",
            timestamp="2020-01-05T12:00:00Z",
            properties={"fake_prop": "value_1"},
        )

        with freeze_time("2020-01-04T13:01:01Z"):
            data = {
                "date_from": "-14d",
                "breakdown": "fake_prop",
                "breakdown_type": "event",
                "display": "ActionsPie",
                "events": [
                    {"id": "watched movie", "name": "watched movie", "type": "events", "order": 0, "math": "dau",}
                ],
            }
            event_response = ClickhouseTrends().run(Filter(data=data), self.team,)
            event_response = sorted(event_response, key=lambda resp: resp["breakdown_value"])

            entity = Entity({"id": "watched movie", "type": "events", "math": "dau"})
            data.update({"breakdown_value": "value_1"})
            people = self._get_trend_people(Filter(data=data), entity)

            # TODO: improve ee/postgres handling
            value_1_ids = sorted(person["id"] for person in people)
            self.assertTrue(
                value_1_ids == sorted([person1.uuid, person2.uuid, person3.uuid])
                or value_1_ids == sorted([person1.pk, person2.pk, person3.pk])
            )

            data.update({"breakdown_value": "value_2"})
            people = self._get_trend_people(Filter(data=data), entity)

            value_2_ids = [person["id"] for person in people]
            self.assertTrue(value_2_ids == [person2.uuid] or value_2_ids == [person2.pk])

    @test_with_materialized_columns(person_properties=["name"])
    def test_breakdown_by_person_property_pie(self):
        self._create_multiple_people()

        with freeze_time("2020-01-04T13:01:01Z"):
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "name",
                        "breakdown_type": "person",
                        "display": "ActionsPie",
                        "events": [
                            {
                                "id": "watched movie",
                                "name": "watched movie",
                                "type": "events",
                                "order": 0,
                                "math": "dau",
                            }
                        ],
                    }
                ),
                self.team,
            )
            event_response = sorted(event_response, key=lambda resp: resp["breakdown_value"])
            self.assertDictContainsSubset({"breakdown_value": "person1", "aggregated_value": 1}, event_response[0])
            self.assertDictContainsSubset({"breakdown_value": "person2", "aggregated_value": 1}, event_response[1])
            self.assertDictContainsSubset({"breakdown_value": "person3", "aggregated_value": 1}, event_response[2])

    @test_with_materialized_columns(event_properties=["key"], person_properties=["name"])
    def test_filter_test_accounts(self):
        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"name": "p1"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )

        p2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p2"], properties={"name": "p2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )
        self.team.test_account_filters = [{"key": "name", "value": "p1", "operator": "is_not", "type": "person"}]
        self.team.save()
        filter = Filter(
            {
                "date_from": "2020-01-01T00:00:00Z",
                "date_to": "2020-01-12T00:00:00Z",
                "events": [{"id": "$pageview", "type": "events", "order": 0}],
                "filter_test_accounts": "true",
            },
            team=self.team,
        )
        result = ClickhouseTrends().run(filter, self.team,)
        self.assertEqual(result[0]["count"], 1)
        filter2 = Filter(
            {
                "date_from": "2020-01-01T00:00:00Z",
                "date_to": "2020-01-12T00:00:00Z",
                "events": [{"id": "$pageview", "type": "events", "order": 0}],
            },
            team=self.team,
        )
        result = ClickhouseTrends().run(filter2, self.team,)
        self.assertEqual(result[0]["count"], 2)
        result = ClickhouseTrends().run(filter.with_data({"breakdown": "key"}), self.team,)
        self.assertEqual(result[0]["count"], 1)

    @test_with_materialized_columns(person_properties=["name"])
    def test_filter_test_accounts_cohorts(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_1"], properties={"name": "John"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_2"], properties={"name": "Jane"})

        _create_event(event="event_name", team=self.team, distinct_id="person_1")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")

        cohort = _create_cohort(
            team=self.team,
            name="cohort1",
            groups=[{"properties": [{"key": "name", "value": "Jane", "type": "person"}]}],
        )
        self.team.test_account_filters = [{"key": "id", "value": cohort.pk, "type": "cohort"}]
        self.team.save()

        response = ClickhouseTrends().run(
            Filter(data={"events": [{"id": "event_name"}], "filter_test_accounts": True}, team=self.team), self.team,
        )

        self.assertEqual(response[0]["count"], 2)
        self.assertEqual(response[0]["data"][-1], 2)

    def test_filter_by_precalculated_cohort(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_1"], properties={"name": "John"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_2"], properties={"name": "Jane"})

        _create_event(event="event_name", team=self.team, distinct_id="person_1")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")

        cohort = _create_cohort(
            team=self.team,
            name="cohort1",
            groups=[{"properties": [{"key": "name", "value": "Jane", "type": "person"}]}],
        )
        cohort.calculate_people_ch()
        with self.settings(USE_PRECALCULATED_CH_COHORT_PEOPLE=True):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "events": [{"id": "event_name"}],
                        "properties": [{"type": "cohort", "key": "id", "value": cohort.pk}],
                    },
                    team=self.team,
                ),
                self.team,
            )

        self.assertEqual(response[0]["count"], 2)
        self.assertEqual(response[0]["data"][-1], 2)

    def test_breakdown_filter_by_precalculated_cohort(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_1"], properties={"name": "John"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person_2"], properties={"name": "Jane"})

        _create_event(event="event_name", team=self.team, distinct_id="person_1")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")
        _create_event(event="event_name", team=self.team, distinct_id="person_2")

        cohort = _create_cohort(
            team=self.team,
            name="cohort1",
            groups=[{"properties": [{"key": "name", "value": "Jane", "type": "person"}]}],
        )
        cohort.calculate_people_ch()

        with self.settings(USE_PRECALCULATED_CH_COHORT_PEOPLE=True):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "events": [{"id": "event_name"}],
                        "properties": [{"type": "cohort", "key": "id", "value": cohort.pk}],
                        "breakdown": "name",
                        "breakdown_type": "person",
                    },
                    team=self.team,
                ),
                self.team,
            )

        self.assertEqual(response[0]["count"], 2)
        self.assertEqual(response[0]["data"][-1], 2)

    def test_bar_chart_by_value(self):
        self._create_events()

        with freeze_time("2020-01-04T13:00:01Z"):
            # with self.assertNumQueries(16):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-7d",
                        "events": [{"id": "sign up"}, {"id": "no events"}],
                        "display": TRENDS_BAR_VALUE,
                    }
                ),
                self.team,
            )
        self.assertEqual(response[0]["aggregated_value"], 4)
        self.assertEqual(response[1]["aggregated_value"], 1)
        self.assertEqual(
            response[0]["days"],
            [
                "2019-12-28",
                "2019-12-29",
                "2019-12-30",
                "2019-12-31",
                "2020-01-01",
                "2020-01-02",
                "2020-01-03",
                "2020-01-04",
            ],
        )

    @test_with_materialized_columns(["$some_property"])
    def test_breakdown_filtering_bar_chart_by_value(self):
        self._create_events()

        # test breakdown filtering
        with freeze_time("2020-01-04T13:01:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-7d",
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0,},],
                        "display": TRENDS_BAR_VALUE,
                    }
                ),
                self.team,
            )

        self.assertEqual(response[0]["aggregated_value"], 2)  # the events without breakdown value
        self.assertEqual(response[1]["aggregated_value"], 1)
        self.assertEqual(response[2]["aggregated_value"], 1)
        self.assertEqual(
            response[0]["days"],
            [
                "2019-12-28",
                "2019-12-29",
                "2019-12-30",
                "2019-12-31",
                "2020-01-01",
                "2020-01-02",
                "2020-01-03",
                "2020-01-04",
            ],
        )

    @test_with_materialized_columns(event_properties=["order"], person_properties=["name"])
    def test_breakdown_with_person_property_filter(self):
        self._create_multiple_people()
        action = _create_action(name="watched movie", team=self.team)

        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "order",
                        "actions": [{"id": action.pk, "type": "actions", "order": 0}],
                        "properties": [{"key": "name", "value": "person2", "type": "person"}],
                    }
                ),
                self.team,
            )
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "order",
                        "events": [
                            {
                                "id": "watched movie",
                                "name": "watched movie",
                                "type": "events",
                                "order": 0,
                                "properties": [{"key": "name", "value": "person2", "type": "person"}],
                            }
                        ],
                    }
                ),
                self.team,
            )

        self.assertDictContainsSubset({"count": 2, "breakdown_value": "2",}, event_response[0])
        self.assertDictContainsSubset({"count": 1, "breakdown_value": "1",}, event_response[1])
        self.assertEntityResponseEqual(event_response, action_response)

    @test_with_materialized_columns(person_properties=["email"])
    def test_breakdown_filtering_persons(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"], properties={"email": "test@posthog.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"], properties={"email": "test@gmail.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person3"], properties={})

        _create_event(event="sign up", distinct_id="person1", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person2", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person3", team=self.team, properties={"key": "val"})
        response = ClickhouseTrends().run(
            Filter(
                data={
                    "date_from": "-14d",
                    "breakdown": "email",
                    "breakdown_type": "person",
                    "events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0,},],
                }
            ),
            self.team,
        )
        self.assertEqual(response[0]["label"], "sign up - none")
        self.assertEqual(response[1]["label"], "sign up - test@gmail.com")
        self.assertEqual(response[2]["label"], "sign up - test@posthog.com")

        self.assertEqual(response[0]["count"], 1)
        self.assertEqual(response[1]["count"], 1)
        self.assertEqual(response[2]["count"], 1)

    # ensure that column names are properly handled when subqueries and person subquery share properties column
    @test_with_materialized_columns(event_properties=["key"], person_properties=["email"])
    def test_breakdown_filtering_persons_with_action_props(self):
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person1"], properties={"email": "test@posthog.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person2"], properties={"email": "test@gmail.com"})
        Person.objects.create(team_id=self.team.pk, distinct_ids=["person3"], properties={})

        _create_event(event="sign up", distinct_id="person1", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person2", team=self.team, properties={"key": "val"})
        _create_event(event="sign up", distinct_id="person3", team=self.team, properties={"key": "val"})
        action = _create_action(
            name="sign up",
            team=self.team,
            properties=[{"key": "key", "type": "event", "value": ["val"], "operator": "exact"}],
        )
        response = ClickhouseTrends().run(
            Filter(
                data={
                    "date_from": "-14d",
                    "breakdown": "email",
                    "breakdown_type": "person",
                    "actions": [{"id": action.pk, "type": "actions", "order": 0}],
                }
            ),
            self.team,
        )
        self.assertEqual(response[0]["label"], "sign up - none")
        self.assertEqual(response[1]["label"], "sign up - test@gmail.com")
        self.assertEqual(response[2]["label"], "sign up - test@posthog.com")

        self.assertEqual(response[0]["count"], 1)
        self.assertEqual(response[1]["count"], 1)
        self.assertEqual(response[2]["count"], 1)

    @test_with_materialized_columns(["$current_url", "$os", "$browser"])
    def test_breakdown_filtering_with_properties(self):
        with freeze_time("2020-01-03T13:01:01Z"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$current_url": "first url", "$browser": "Firefox", "$os": "Mac"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$current_url": "first url", "$browser": "Chrome", "$os": "Windows"},
            )
        with freeze_time("2020-01-04T13:01:01Z"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$current_url": "second url", "$browser": "Firefox", "$os": "Mac"},
            )
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$current_url": "second url", "$browser": "Chrome", "$os": "Windows"},
            )

        with freeze_time("2020-01-05T13:01:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "date_from": "-14d",
                        "breakdown": "$current_url",
                        "events": [
                            {
                                "id": "sign up",
                                "name": "sign up",
                                "type": "events",
                                "order": 0,
                                "properties": [{"key": "$os", "value": "Mac"}],
                            },
                        ],
                        "properties": [{"key": "$browser", "value": "Firefox"}],
                    }
                ),
                self.team,
            )

        response = sorted(response, key=lambda x: x["label"])
        self.assertEqual(response[0]["label"], "sign up - first url")
        self.assertEqual(response[1]["label"], "sign up - second url")

        self.assertEqual(sum(response[0]["data"]), 1)
        self.assertEqual(response[0]["breakdown_value"], "first url")

        self.assertEqual(sum(response[1]["data"]), 1)
        self.assertEqual(response[1]["breakdown_value"], "second url")

    @test_with_materialized_columns(["$os", "$some_property"])
    def test_dau_with_breakdown_filtering_with_prop_filter(self):
        sign_up_action, _ = self._create_events()
        with freeze_time("2020-01-02T13:01:01Z"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$some_property": "other_value", "$os": "Windows"},
            )
        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "breakdown": "$some_property",
                        "actions": [{"id": sign_up_action.id, "math": "dau"}],
                        "properties": [{"key": "$os", "value": "Windows"}],
                    }
                ),
                self.team,
            )
            event_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "breakdown": "$some_property",
                        "events": [{"id": "sign up", "math": "dau"}],
                        "properties": [{"key": "$os", "value": "Windows"}],
                    }
                ),
                self.team,
            )

        self.assertEqual(event_response[0]["label"], "sign up - other_value")

        self.assertEqual(sum(event_response[0]["data"]), 1)
        self.assertEqual(event_response[0]["data"][5], 1)  # property not defined

        self.assertEntityResponseEqual(action_response, event_response)

    @test_with_materialized_columns(event_properties=["$host"], person_properties=["$some_prop"])
    def test_against_clashing_entity_and_property_filter_naming(self):
        # Regression test for https://github.com/PostHog/posthog/issues/5814
        Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="blabla",
            properties={"$host": "app.example.com"},
            timestamp="2020-01-03T12:00:00Z",
        )

        with freeze_time("2020-01-04T13:01:01Z"):
            response = ClickhouseTrends().run(
                Filter(
                    data={
                        "events": [
                            {
                                "id": "$pageview",
                                "properties": [{"key": "$host", "operator": "icontains", "value": ".com"}],
                            }
                        ],
                        "properties": [{"key": "$host", "value": ["app.example.com", "another.com"]}],
                        "breakdown": "$some_prop",
                        "breakdown_type": "person",
                    }
                ),
                self.team,
            )

        self.assertEqual(response[0]["count"], 1)

    # this ensures that the properties don't conflict when formatting params
    @test_with_materialized_columns(["$current_url"])
    def test_action_with_prop(self):
        person = Person.objects.create(
            team_id=self.team.pk, distinct_ids=["blabla", "anonymous_id"], properties={"$some_prop": "some_val"}
        )
        sign_up_action = Action.objects.create(team=self.team, name="sign up")
        ActionStep.objects.create(
            action=sign_up_action, event="sign up", properties={"$current_url": "https://posthog.com/feedback/1234"}
        )

        with freeze_time("2020-01-02T13:01:01Z"):
            _create_event(
                team=self.team,
                event="sign up",
                distinct_id="blabla",
                properties={"$current_url": "https://posthog.com/feedback/1234"},
            )

        with freeze_time("2020-01-04T13:01:01Z"):
            action_response = ClickhouseTrends().run(
                Filter(
                    data={
                        "actions": [{"id": sign_up_action.id, "math": "dau"}],
                        "properties": [{"key": "$current_url", "value": "fake"}],
                    }
                ),
                self.team,
            )

        # if the params were shared it would be 1 because action would take precedence
        self.assertEqual(action_response[0]["count"], 0)

    @test_with_materialized_columns(["$current_url"], verify_no_jsonextract=False)
    def test_combine_all_cohort_and_icontains(self):
        # This caused some issues with SQL parsing
        sign_up_action, _ = self._create_events()
        cohort = Cohort.objects.create(team=self.team, name="a", groups=[{"properties": {"key": "value"}}])
        action_response = ClickhouseTrends().run(
            Filter(
                data={
                    "actions": [{"id": sign_up_action.id, "math": "dau"}],
                    "properties": [{"key": "$current_url", "value": "ii", "operator": "icontains"}],
                    "breakdown": [cohort.pk, "all"],
                    "breakdown_type": "cohort",
                }
            ),
            self.team,
        )
        self.assertEqual(action_response[0]["count"], 0)

    def _create_active_user_events(self):
        p0 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p0"], properties={"name": "p1"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p0",
            timestamp="2020-01-03T12:00:00Z",
            properties={"key": "val"},
        )

        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"name": "p1"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-09T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-10T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )

        p2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p2"], properties={"name": "p2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-09T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )

    def test_active_user_math(self):
        self._create_active_user_events()

        data = {
            "date_from": "2020-01-09T00:00:00Z",
            "date_to": "2020-01-16T00:00:00Z",
            "events": [{"id": "$pageview", "type": "events", "order": 0, "math": "weekly_active"}],
        }

        filter = Filter(data=data)
        result = ClickhouseTrends().run(filter, self.team,)
        self.assertEqual(result[0]["data"], [3.0, 2.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    def test_active_user_math_action(self):
        action = _create_action(name="$pageview", team=self.team)
        self._create_active_user_events()

        data = {
            "date_from": "2020-01-09T00:00:00Z",
            "date_to": "2020-01-16T00:00:00Z",
            "actions": [{"id": action.id, "type": "actions", "order": 0, "math": "weekly_active"}],
        }

        filter = Filter(data=data)
        result = ClickhouseTrends().run(filter, self.team,)
        self.assertEqual(result[0]["data"], [3.0, 2.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    @test_with_materialized_columns(["key"])
    def test_breakdown_active_user_math(self):

        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"name": "p1"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-09T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-10T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )

        p2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p2"], properties={"name": "p2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-09T12:00:00Z",
            properties={"key": "val"},
        )
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-11T12:00:00Z",
            properties={"key": "val"},
        )

        data = {
            "date_from": "2020-01-01T00:00:00Z",
            "date_to": "2020-01-12T00:00:00Z",
            "breakdown": "key",
            "events": [{"id": "$pageview", "type": "events", "order": 0, "math": "weekly_active"}],
        }

        filter = Filter(data=data)
        result = ClickhouseTrends().run(filter, self.team,)
        self.assertEqual(result[0]["data"], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 2.0, 2.0, 0.0])

    @test_with_materialized_columns(person_properties=["key", "key_2"], verify_no_jsonextract=False)
    def test_breakdown_multiple_cohorts(self):
        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"key": "value"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        p2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p2"], properties={"key_2": "value_2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        p3 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p3"], properties={"key_2": "value_2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p3",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        cohort1 = _create_cohort(
            team=self.team,
            name="cohort_1",
            groups=[{"properties": [{"key": "key", "value": "value", "type": "person"}]}],
        )
        cohort2 = _create_cohort(
            team=self.team,
            name="cohort_2",
            groups=[{"properties": [{"key": "key_2", "value": "value_2", "type": "person"}]}],
        )

        cohort1.calculate_people()
        cohort1.calculate_people_ch()

        cohort2.calculate_people()
        cohort2.calculate_people_ch()

        with self.settings(USE_PRECALCULATED_CH_COHORT_PEOPLE=True):  # Normally this is False in tests
            with freeze_time("2020-01-04T13:01:01Z"):
                res = ClickhouseTrends().run(
                    Filter(
                        data={
                            "date_from": "-7d",
                            "events": [{"id": "$pageview"}],
                            "properties": [],
                            "breakdown": [cohort1.pk, cohort2.pk],
                            "breakdown_type": "cohort",
                        }
                    ),
                    self.team,
                )

        self.assertEqual(res[0]["count"], 1)
        self.assertEqual(res[1]["count"], 2)

    @test_with_materialized_columns(person_properties=["key", "key_2"], verify_no_jsonextract=False)
    def test_breakdown_single_cohort(self):
        p1 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p1"], properties={"key": "value"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p1",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        p2 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p2"], properties={"key_2": "value_2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p2",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        p3 = Person.objects.create(team_id=self.team.pk, distinct_ids=["p3"], properties={"key_2": "value_2"})
        _create_event(
            team=self.team,
            event="$pageview",
            distinct_id="p3",
            timestamp="2020-01-02T12:00:00Z",
            properties={"key": "val"},
        )

        cohort1 = _create_cohort(
            team=self.team,
            name="cohort_1",
            groups=[{"properties": [{"key": "key", "value": "value", "type": "person"}]}],
        )

        cohort1.calculate_people()
        cohort1.calculate_people_ch()

        with self.settings(USE_PRECALCULATED_CH_COHORT_PEOPLE=True):  # Normally this is False in tests
            with freeze_time("2020-01-04T13:01:01Z"):
                res = ClickhouseTrends().run(
                    Filter(
                        data={
                            "date_from": "-7d",
                            "events": [{"id": "$pageview"}],
                            "properties": [],
                            "breakdown": cohort1.pk,
                            "breakdown_type": "cohort",
                        }
                    ),
                    self.team,
                )

        self.assertEqual(res[0]["count"], 1)

    @test_with_materialized_columns(["key", "$current_url"])
    def test_filtering_with_action_props(self):
        _create_event(
            event="sign up",
            distinct_id="person1",
            team=self.team,
            properties={"key": "val", "$current_url": "/some/page"},
        )
        _create_event(
            event="sign up",
            distinct_id="person2",
            team=self.team,
            properties={"key": "val", "$current_url": "/some/page"},
        )
        _create_event(
            event="sign up",
            distinct_id="person3",
            team=self.team,
            properties={"key": "val", "$current_url": "/another/page"},
        )

        action = Action.objects.create(name="sign up", team=self.team)
        ActionStep.objects.create(
            action=action,
            event="sign up",
            url="/some/page",
            properties=[{"key": "key", "type": "event", "value": ["val"], "operator": "exact"}],
        )

        response = ClickhouseTrends().run(
            Filter(data={"date_from": "-14d", "actions": [{"id": action.pk, "type": "actions", "order": 0}],}),
            self.team,
        )

        self.assertEqual(response[0]["count"], 2)

    def test_trends_math_without_math_property(self):
        with self.assertRaises(ValidationError):
            ClickhouseTrends().run(
                Filter(data={"events": [{"id": "sign up", "math": "sum"}]}), self.team,
            )

    @patch("posthog.queries.trends.clickhouse_trends.sync_execute")
    def test_should_throw_exception(self, patch_sync_execute):
        self._create_events()
        patch_sync_execute.side_effect = Exception()
        # test breakdown filtering
        with self.assertRaises(Exception):
            with self.settings(TEST=False, DEBUG=False):
                response = ClickhouseTrends().run(
                    Filter(data={"events": [{"id": "sign up", "name": "sign up", "type": "events", "order": 0,},],}),
                    self.team,
                )
