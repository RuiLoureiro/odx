import datetime
from tqdm.auto import tqdm
from collections import defaultdict
from rich import print
from .bus_schedule import BusSchedule, BusRouteTuple
from .metro_schedule import MetroSchedule
from .config import ODXConfig
from .geo import StopsDistance


class ODX_ENUMS:
    METRO = "metro"
    BUS = "bus"
    METRO_OUT = "OUT"
    METRO_IN = "IN"


class BusStage:
    mode = "bus"

    def __init__(
        self,
        boarding,
    ):
        self.boarding = boarding
        self.entry_ts = boarding.timestamp
        self.entry_stop = BusSchedule().get_stop(boarding.stop_id)
        self.route = self.route_from_transaction(boarding)
        self.exit_ts = None
        self.exit_stop = None

    @staticmethod
    def route_from_transaction(transaction):
        route_tup = BusRouteTuple(
            transaction.route_id,
            transaction.route_direction,
            transaction.route_variant,
        )
        return BusSchedule().get_route(route_tup)

    def __repr__(self):
        return f"[BUS] [{self.entry_ts}] ({self.entry_stop}) -> [{self.exit_ts }] ({self.exit_stop}) [{self.route}]"


class MetroStage:
    mode = "metro"

    def __init__(
        self,
        boarding,
        alighting,
    ):
        self.boarding = boarding
        self.alighting = alighting
        if boarding:
            self.entry_ts = boarding.timestamp
            self.entry_stop = MetroSchedule().get_stop(boarding.stop_id)
        else:
            self.entry_ts = None
            self.entry_stop = None

        if alighting:
            self.exit_ts = alighting.timestamp
            self.exit_stop = MetroSchedule().get_stop(alighting.stop_id)
        else:
            self.exit_ts = None
            self.exit_stop = None

    def __repr__(self):
        return f"[METRO] [{self.entry_ts}] ({self.entry_stop}) -> [{self.exit_ts }] ({self.exit_stop})"


class ODX:
    """
    Computes odx using bus and metro afc data.
    """

    def __init__(self):
        self.stops_distance = StopsDistance(self.stops)
        self.bus_schedule = BusSchedule()
        self.metro_schedule = MetroSchedule()

    @staticmethod
    def get_record_day(row):
        time = row.timestamp.time()
        date = row.timestamp.date()
        time_limit = ODXConfig.NEW_DAY_TIME

        if time < time_limit:
            return date - datetime.timedelta(days=1)
        else:
            return date

    def get_stages(self, afc):
        """
        Builds stages
        Metro stages are built from 2 afc records (entry and exit)
        Bus stages are built from a single afc record (boarding).
        Divides stages by date and by card_id
        """
        stages = defaultdict(lambda: defaultdict(list))
        print("Splitting dataframe by card id and date..")
        transactions = defaultdict(lambda: defaultdict(list))
        for row in tqdm(afc.itertuples(), total=len(afc)):
            transactions[row.card_id][self.get_record_day(row)].append(row)
        print(
            f"Processing {len(transactions)} transactions.., between {afc.timestamp.iloc[0]} and {afc.timestamp.iloc[-1]}.."
        )
        for cid in tqdm(transactions):
            for date in transactions[cid]:
                iter_ = enumerate(transactions[cid][date])
                for idx, transaction in iter_:
                    if transaction.mode == ODX_ENUMS.METRO:
                        try:
                            next_transaction = transactions[cid][date][idx + 1]
                        except IndexError:
                            next_transaction = None
                        if transaction.way == ODX_ENUMS.METRO_IN:
                            if (
                                next_transaction
                                and next_transaction.mode == ODX_ENUMS.METRO
                                and next_transaction.way == ODX_ENUMS.METRO_OUT
                            ):
                                stage = MetroStage(
                                    transaction, next_transaction
                                )
                                # since we already processed the next transaction, skip it
                                next(iter_)
                            else:
                                stage = MetroStage(transaction, None)

                        if transaction.way == ODX_ENUMS.METRO_OUT:
                            stage = MetroStage(None, transaction)
                    elif transaction.mode == ODX_ENUMS.BUS:
                        stage = BusStage(transaction)

                    if (stage.mode == "metro") and (
                        stage.entry_stop == stage.exit_stop
                    ):
                        continue

                    stages[cid][date].append(stage)

        return stages
