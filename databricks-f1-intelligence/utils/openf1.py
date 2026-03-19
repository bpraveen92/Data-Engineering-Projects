"""
OpenF1 API client.

Historical data (2023+) is available without authentication.
Docs: https://openf1.org/. All methods return plain list[dict].
"""

import time
import logging
import requests

BASE_URL = "https://api.openf1.org/v1"
REQUEST_DELAY = 0.3
TIMEOUT = 30

logger = logging.getLogger(__name__)


def _s(v, default=""):
    """Return str(v) unless v is None, in which case return default.

    Prevents str(None) == 'None' from polluting Bronze string columns.
    """
    return default if v is None else str(v)


class OpenF1Client:
    """
    HTTP client for the OpenF1 REST API (https://openf1.org).

    OpenF1 organises telemetry data by session_key rather than by season/round.
    fetch_and_upload.py resolves session_key → round by joining OpenF1 session
    dates against the Jolpica race calendar on race_date (YYYY-MM-DD).

    Historical data from 2023 onwards is freely available without authentication.
    All field values are returned as strings to match the Bronze layer schema.
    """

    def __init__(self, delay=REQUEST_DELAY):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def call_api(self, endpoint, params=None):
        """
        Make a GET request to the OpenF1 API and return the parsed JSON list.

        Returns an empty list on 404. All other HTTP errors raise via
        raise_for_status(). The delay between calls prevents rate-limit issues
        during the ~480-call lap fetch for a full 2024 season.

        Example:
            client.call_api("laps", {"session_key": 9158, "driver_number": 44})
            # → [{"lap_number": 1, "lap_duration": 102.143, ...}, ...]
        """
        url = f"{BASE_URL}/{endpoint}"
        for attempt in range(4):
            try:
                resp = self.session.get(url, params=params or {}, timeout=TIMEOUT)
                if resp.status_code == 404:
                    logger.warning("404 for %s params=%s", url, params)
                    return []
                if resp.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    logger.warning("429 rate limit for %s — retrying in %ds (attempt %d/3)", url, wait, attempt + 1)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                time.sleep(self.delay)
                return resp.json()
            except requests.RequestException as e:
                logger.error("Request failed for %s: %s", url, e)
                raise
        logger.error("Exhausted retries for %s", url)
        raise RuntimeError(f"429 rate limit — exhausted retries for {url}")

    def fetch_race_sessions(self, year):
        """
        Fetch metadata for every Race session in a calendar year.

        Used by fetch_and_upload.py to build the session_key → round mapping.
        The date_start field is truncated to YYYY-MM-DD and matched against the
        Jolpica race_date column to resolve the correct round number.

        Example row (Bahrain 2024):
            {
                "session_key": "9158", "session_name": "Race",
                "year": "2024", "date_start": "2024-03-02T15:00:00+00:00",
                "circuit_key": "3", "circuit_short_name": "Bahrain",
                "country_name": "Bahrain",
            }
        """
        results = self.call_api("sessions", params={"year": year, "session_name": "Race"})
        rows = []
        for s in results:
            rows.append({
                "session_key":        str(s.get("session_key", "")),
                "session_name":       s.get("session_name", ""),
                "year":               str(s.get("year", year)),
                "date_start":         s.get("date_start", ""),
                "circuit_key":        str(s.get("circuit_key", "")),
                "circuit_short_name": s.get("circuit_short_name", ""),
                "country_name":       s.get("country_name", ""),
            })
        return rows

    def fetch_drivers_for_session(self, session_key):
        """
        Fetch driver metadata for all participants in a session.

        Called once per session to get the list of driver_numbers before
        looping through per-driver lap fetches. Returns ~20 rows per session.

        Example row:
            {
                "session_key": "9158", "driver_number": "44",
                "broadcast_name": "L HAMILTON", "full_name": "Lewis Hamilton",
                "name_acronym": "HAM", "team_name": "Mercedes",
                "team_colour": "27F4D2", "country_code": "GBR",
            }
        """
        results = self.call_api("drivers", params={"session_key": session_key})
        rows = []
        for d in results:
            rows.append({
                "session_key":    str(session_key),
                "driver_number":  str(d.get("driver_number", "")),
                "broadcast_name": d.get("broadcast_name", ""),
                "full_name":      d.get("full_name", ""),
                "name_acronym":   d.get("name_acronym", ""),
                "team_name":      d.get("team_name", ""),
                "team_colour":    d.get("team_colour", ""),
                "country_code":   d.get("country_code", ""),
            })
        return rows

    def fetch_laps(self, session_key, driver_number):
        """
        Fetch per-lap timing data for one driver in one session.

        This is the most granular and voluminous call — ~57 rows per driver
        per race. For a full 2024 season (~20 drivers × 24 sessions) this
        function is called ~480 times, yielding ~26 000 rows total.

        The Bronze MERGE key is (session_key, driver_number, lap_number).
        Sector times and speed trap readings are also captured for later
        performance analysis in Silver.

        Example row (Hamilton, lap 3, Bahrain 2024):
            {
                "session_key": "9158", "driver_number": "44", "lap_number": "3",
                "lap_duration": "97.814", "is_pit_out_lap": "False",
                "date_start": "2024-03-02T15:09:43.425000+00:00",
                "duration_sector_1": "29.512", "duration_sector_2": "39.241",
                "duration_sector_3": "29.061", "i1_speed": "298",
                "i2_speed": "265", "st_speed": "317",
            }
        """
        results = self.call_api("laps", params={
            "session_key":   session_key,
            "driver_number": driver_number,
        })
        rows = []
        for lap in results:
            rows.append({
                "session_key":       str(session_key),
                "driver_number":     str(driver_number),
                "lap_number":        _s(lap.get("lap_number")),
                "lap_duration":      _s(lap.get("lap_duration")),
                "is_pit_out_lap":    _s(lap.get("is_pit_out_lap")),
                "date_start":        lap.get("date_start") or "",
                "duration_sector_1": _s(lap.get("duration_sector_1")),
                "duration_sector_2": _s(lap.get("duration_sector_2")),
                "duration_sector_3": _s(lap.get("duration_sector_3")),
                "i1_speed":          _s(lap.get("i1_speed")),
                "i2_speed":          _s(lap.get("i2_speed")),
                "st_speed":          _s(lap.get("st_speed")),
            })
        return rows

    def fetch_stints(self, session_key):
        """
        Fetch tyre stint data for all drivers in a session.

        A stint is a continuous period on one set of tyres. A typical race has
        2–3 stints per driver, giving ~40–60 rows per session. The Silver
        notebook joins stints to laps on (session_key, driver_number,
        lap_start <= lap_number <= lap_end) to attach compound and tyre age
        to every individual lap.

        Example row (Verstappen, Stint 1, Bahrain 2024):
            {
                "session_key": "9158", "driver_number": "1",
                "stint_number": "1", "lap_start": "1", "lap_end": "20",
                "compound": "MEDIUM", "tyre_age_at_start": "0",
            }
        """
        results = self.call_api("stints", params={"session_key": session_key})
        rows = []
        for s in results:
            rows.append({
                "session_key":       str(session_key),
                "driver_number":     _s(s.get("driver_number")),
                "stint_number":      _s(s.get("stint_number")),
                "lap_start":         _s(s.get("lap_start")),
                "lap_end":           _s(s.get("lap_end")),
                "compound":          s.get("compound") or "",
                "tyre_age_at_start": _s(s.get("tyre_age_at_start")),
            })
        return rows

    def fetch_pit_events(self, session_key):
        """
        Fetch pit lane entry/exit events for a session from the OpenF1 API.

        Note: pit stop durations from OpenF1 complement the Jolpica pit_stops
        table, which is sourced from official timing data. This method is
        available for optional enrichment but is not part of the primary
        Bronze ingestion flow.

        Example row:
            {
                "session_key": "9158", "driver_number": "1",
                "lap_number": "20", "pit_duration": "24.5",
                "date": "2024-03-02T15:48:17.123000+00:00",
            }
        """
        results = self.call_api("pit", params={"session_key": session_key})
        rows = []
        for p in results:
            rows.append({
                "session_key":   str(session_key),
                "driver_number": str(p.get("driver_number", "")),
                "lap_number":    str(p.get("lap_number", "")),
                "pit_duration":  str(p.get("pit_duration", "")),
                "date":          p.get("date", ""),
            })
        return rows

    def fetch_weather(self, session_key):
        """
        Fetch time-series weather readings sampled throughout a session.

        OpenF1 records a weather snapshot roughly every 30 seconds, producing
        100–200 rows per race session. Not ingested in the primary Bronze flow
        but available for future enrichment of the lap analysis table.

        Example row:
            {
                "session_key": "9158", "date": "2024-03-02T15:03:00+00:00",
                "air_temperature": "29.5", "track_temperature": "38.1",
                "humidity": "57.0", "wind_speed": "3.0",
                "wind_direction": "155", "rainfall": "0",
            }
        """
        results = self.call_api("weather", params={"session_key": session_key})
        rows = []
        for w in results:
            rows.append({
                "session_key":       str(session_key),
                "date":              w.get("date", ""),
                "air_temperature":   str(w.get("air_temperature", "")),
                "track_temperature": str(w.get("track_temperature", "")),
                "humidity":          str(w.get("humidity", "")),
                "wind_speed":        str(w.get("wind_speed", "")),
                "wind_direction":    str(w.get("wind_direction", "")),
                "rainfall":          str(w.get("rainfall", "")),
            })
        return rows
