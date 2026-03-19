"""
Jolpica-F1 API client.

Jolpica is the maintained successor to the Ergast API, with backward-compatible
endpoints at https://api.jolpi.ca/ergast/f1/. All methods return plain list[dict]
so this module runs both locally and on the cluster without PySpark.
"""

import time
import logging
import requests

BASE_URL = "https://api.jolpi.ca/ergast/f1"
REQUEST_DELAY = 1.0  # seconds between calls — Jolpica is a shared public service
TIMEOUT = 30

logger = logging.getLogger(__name__)


class JolpicaClient:
    """
    HTTP client for the Jolpica-F1 REST API.

    Wraps every endpoint used by the pipeline and returns plain list[dict]
    rows ready for pandas / PySpark consumption. All string values are kept
    as strings at this layer — type casting happens in the Silver notebook.

    Rate limiting: a configurable delay (default 0.5 s) is injected between
    calls so we stay within Jolpica's fair-use policy for a shared public
    service.
    """

    def __init__(self, delay=REQUEST_DELAY):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def call_api(self, path):
        """
        Make a GET request to the Jolpica API and return the parsed JSON body.

        Returns None on 404 (e.g. sprint-weekend qualifying, which Jolpica
        does not serve). Retries up to 3 times on 429 (rate limit) with
        exponential backoff (10 s, 20 s, 40 s). All other HTTP errors raise.

        Example:
            client.call_api("/2024/1/results.json?limit=25")
            # → {"MRData": {"RaceTable": {"Races": [...]}}}
        """
        url = f"{BASE_URL}{path}"
        for attempt in range(4):
            try:
                resp = self.session.get(url, timeout=TIMEOUT)
                if resp.status_code == 404:
                    logger.warning("404 for %s — returning empty", url)
                    return None
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

    def fetch_races(self, season):
        """
        Fetch the full race calendar and circuit metadata for a season.

        Returns one row per race. For 2024 that is 24 rows.

        Example row:
            {
                "season": "2024", "round": "1", "race_name": "Bahrain Grand Prix",
                "circuit_id": "bahrain", "circuit_name": "Bahrain International Circuit",
                "locality": "Sakhir", "country": "Bahrain",
                "lat": "26.0325", "lon": "50.5106",
                "race_date": "2024-03-02", "race_time": "15:00:00Z",
            }
        """
        data = self.call_api(f"/{season}/races.json?limit=50")
        if not data:
            return []
        rows = []
        for r in data["MRData"]["RaceTable"]["Races"]:
            circuit = r.get("Circuit", {})
            loc = circuit.get("Location", {})
            rows.append({
                "season":       str(r.get("season", season)),
                "round":        str(r.get("round", "")),
                "race_name":    r.get("raceName", ""),
                "circuit_id":   circuit.get("circuitId", ""),
                "circuit_name": circuit.get("circuitName", ""),
                "locality":     loc.get("locality", ""),
                "country":      loc.get("country", ""),
                "lat":          loc.get("lat", ""),
                "lon":          loc.get("long", ""),
                "race_date":    r.get("date", ""),
                "race_time":    r.get("time", ""),
            })
        return rows

    def fetch_results(self, season, round_num):
        """
        Fetch race classification for one round — one row per classified/retired driver.

        The MERGE key for this table is (season, round, driver_id). Post-race
        steward penalties that change a driver's position or points will update
        an existing row on the next pipeline run — the primary upsert scenario.

        Example row (Bahrain 2024, Verstappen):
            {
                "season": "2024", "round": "1", "driver_id": "max_verstappen",
                "driver_code": "VER", "driver_number": "1",
                "constructor_id": "red_bull", "grid_position": "1",
                "final_position": "1", "position_text": "1", "points": "25",
                "laps_completed": "57", "race_time": "1:31:44.742",
                "fastest_lap_rank": "2", "fastest_lap_time": "1:32.608",
                "fastest_lap_speed": "206.018", "status": "Finished",
            }
        """
        data = self.call_api(f"/{season}/{round_num}/results.json?limit=25")
        if not data:
            return []
        races = data["MRData"]["RaceTable"]["Races"]
        if not races:
            return []
        rows = []
        for result in races[0].get("Results", []):
            driver = result.get("Driver", {})
            constructor = result.get("Constructor", {})
            fastest = result.get("FastestLap", {})
            avg_speed = fastest.get("AverageSpeed", {})
            rows.append({
                "season":             str(season),
                "round":              str(round_num),
                "driver_id":          driver.get("driverId", ""),
                "driver_code":        driver.get("code", ""),
                "driver_number":      result.get("number", ""),
                "constructor_id":     constructor.get("constructorId", ""),
                "grid_position":      result.get("grid", ""),
                "final_position":     result.get("position", ""),
                "position_text":      result.get("positionText", ""),
                "points":             result.get("points", ""),
                "laps_completed":     result.get("laps", ""),
                "race_time":          result.get("Time", {}).get("time", ""),
                "race_time_millis":   result.get("Time", {}).get("millis", ""),
                "fastest_lap_rank":   fastest.get("rank", ""),
                "fastest_lap_time":   fastest.get("Time", {}).get("time", ""),
                "fastest_lap_speed":  avg_speed.get("speed", ""),
                "status":             result.get("status", ""),
            })
        return rows

    def fetch_qualifying(self, season, round_num):
        """
        Fetch qualifying session results for one round — one row per driver.

        Returns an empty list for sprint-format weekends (6 of 24 rounds in
        2024) because Jolpica serves a 404 for those rounds. The Silver
        notebook handles this gracefully with an early exit.

        Example row (Bahrain 2024, Verstappen — pole):
            {
                "season": "2024", "round": "1", "driver_id": "max_verstappen",
                "driver_code": "VER", "constructor_id": "red_bull",
                "qualifying_position": "1",
                "q1_time": "1:29.921", "q2_time": "1:28.887", "q3_time": "1:29.179",
            }
        """
        data = self.call_api(f"/{season}/{round_num}/qualifying.json?limit=25")
        if not data:
            return []
        races = data["MRData"]["RaceTable"]["Races"]
        if not races:
            return []
        rows = []
        for q in races[0].get("QualifyingResults", []):
            driver = q.get("Driver", {})
            constructor = q.get("Constructor", {})
            rows.append({
                "season":               str(season),
                "round":                str(round_num),
                "driver_id":            driver.get("driverId", ""),
                "driver_code":          driver.get("code", ""),
                "constructor_id":       constructor.get("constructorId", ""),
                "qualifying_position":  q.get("position", ""),
                "q1_time":              q.get("Q1", ""),
                "q2_time":              q.get("Q2", ""),
                "q3_time":              q.get("Q3", ""),
            })
        return rows

    def fetch_pit_stops(self, season, round_num):
        """
        Fetch pit stop events for one round — one row per stop per driver.

        A driver who stops twice produces two rows (stop_number "1" and "2").
        The MERGE key is (season, round, driver_id, stop_number).

        Example row:
            {
                "season": "2024", "round": "1", "driver_id": "max_verstappen",
                "stop_number": "1", "lap": "27",
                "time_of_day": "16:14:22", "duration": "2.412", "duration_millis": "",
            }
        """
        data = self.call_api(f"/{season}/{round_num}/pitstops.json?limit=100")
        if not data:
            return []
        races = data["MRData"]["RaceTable"]["Races"]
        if not races:
            return []
        rows = []
        for ps in races[0].get("PitStops", []):
            rows.append({
                "season":          str(season),
                "round":           str(round_num),
                "driver_id":       ps.get("driverId", ""),
                "stop_number":     ps.get("stop", ""),
                "lap":             ps.get("lap", ""),
                "time_of_day":     ps.get("time", ""),
                "duration":        ps.get("duration", ""),
                "duration_millis": "",
            })
        return rows

    def fetch_driver_standings(self, season, round_num):
        """
        Fetch the WDC standings snapshot after a specific round.

        Returns one row per driver (20 for 2024). The Silver notebook adds a
        position_change column via a LAG window over season + driver ordered
        by round.

        Example row (after Bahrain 2024):
            {
                "season": "2024", "round": "1", "driver_id": "max_verstappen",
                "driver_code": "VER", "constructor_id": "red_bull",
                "position": "1", "points": "25", "wins": "1",
            }
        """
        data = self.call_api(f"/{season}/{round_num}/driverStandings.json")
        if not data:
            return []
        lists = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not lists:
            return []
        rows = []
        for entry in lists[0].get("DriverStandings", []):
            driver = entry.get("Driver", {})
            constructors = entry.get("Constructors", [])
            constructor_id = constructors[0].get("constructorId", "") if constructors else ""
            rows.append({
                "season":         str(season),
                "round":          str(round_num),
                "driver_id":      driver.get("driverId", ""),
                "driver_code":    driver.get("code", ""),
                "constructor_id": constructor_id,
                "position":       entry.get("position", ""),
                "points":         entry.get("points", ""),
                "wins":           entry.get("wins", ""),
            })
        return rows

    def fetch_constructor_standings(self, season, round_num):
        """
        Fetch the WCC standings snapshot after a specific round.

        Returns one row per constructor (10 for 2024).

        Example row (after Bahrain 2024):
            {
                "season": "2024", "round": "1", "constructor_id": "red_bull",
                "constructor_name": "Red Bull", "nationality": "Austrian",
                "position": "1", "points": "40", "wins": "1",
            }
        """
        data = self.call_api(f"/{season}/{round_num}/constructorStandings.json")
        if not data:
            return []
        lists = data["MRData"]["StandingsTable"]["StandingsLists"]
        if not lists:
            return []
        rows = []
        for entry in lists[0].get("ConstructorStandings", []):
            constructor = entry.get("Constructor", {})
            rows.append({
                "season":           str(season),
                "round":            str(round_num),
                "constructor_id":   constructor.get("constructorId", ""),
                "constructor_name": constructor.get("name", ""),
                "nationality":      constructor.get("nationality", ""),
                "position":         entry.get("position", ""),
                "points":           entry.get("points", ""),
                "wins":             entry.get("wins", ""),
            })
        return rows

    def get_round_count(self, season):
        """
        Return the total number of race rounds in a season.

        Derived by counting entries in the race calendar — no extra API call.
        Used by fetch_and_upload.py to build the full list of rounds when
        --round is not specified.

        Example: get_round_count(2024) → 24
        """
        return len(self.fetch_races(season))
