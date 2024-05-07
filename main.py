from pymongo import MongoClient
import datetime


client = MongoClient('mongodb://localhost:27017/')
database = client['sampleDB']
collection = database['sample_collection']


class AggregateManager:
    """
    Class for managing different aggregation ways depending on the input data.
    Provided 3 defined pipelines and methods to process them.
    """
    def __init__(self, dt_from: str, dt_upto: str, group_type: str):
        self.dt_from = self.__convert_dates(dt_from)
        self.dt_upto = self.__convert_dates(dt_upto)
        self.group_type = group_type

    @staticmethod
    def __convert_dates(ISO_date: str) -> datetime.datetime:
        """
        Convert given dates to datetime obj.

        :param ISO_date: Source datetime string in ISO format.
        """
        datetime_obj = datetime.datetime.strptime(ISO_date, '%Y-%m-%dT%H:%M:%S')
        return datetime_obj

    def get_group_type(self) -> str:
        """Match the resulting value with the grouping pymongo operators."""
        group_type = f'${self.group_type}'
        if self.group_type == 'day':
            group_type = '$dayOfYear'
        return group_type

    def get_hours_list(self) -> list:
        """Calculate hours amount in specified period and return list of them."""
        days = self.dt_upto - self.dt_from
        hours = days.seconds // 3600
        end_hour = days.days * 24 + hours
        hours_numbers = list(range(end_hour + 1))
        return hours_numbers

    def get_hours_range(self) -> list:
        """Create list of datetime objects based on list of hours and return list of them."""
        hours_range = []
        day = self.dt_from.day
        cnt_hour = 0
        for hour in self.get_hours_list():
            if cnt_hour == 24:
                day += 1
                cnt_hour = 0
            hours_range.append(datetime.datetime(self.dt_from.year, self.dt_from.month, day, cnt_hour))
            cnt_hour += 1
        return hours_range

    def get_days_range(self) -> list:
        """Create list of datetime objects based on given source dates and return list of them."""
        months_numbers = list(range(self.dt_from.month, self.dt_upto.month + 1))
        dates_range = [datetime.datetime(self.dt_from.year, month, self.dt_from.day) for month in months_numbers]
        if self.group_type == 'day':
            dates_range = [self.dt_from + datetime.timedelta(days=i)
                           for i in range((self.dt_upto - self.dt_from).days + 1)]
        return dates_range

    def get_pipeline(self) -> list:
        """Set up pipeline with defined stages and return ready pipeline."""
        pipeline = [
            {'$match':
                {'dt':
                    {'$gte': self.dt_from, '$lt': self.dt_upto}
                 }
             },
            {'$group':
                {'_id': {self.get_group_type(): '$dt'},
                 'total': {'$sum': '$value'}
                 }
             },
            {'$sort':
                {'_id': 1}
             }
        ]
        return pipeline

    def get_cumulative_data(self) -> dict:
        """Apply aggregation using processed pipeline, create and return ready result dict."""
        agg_result = {str(item['_id']): item['total'] for item in collection.aggregate(self.get_pipeline())}
        date_range = self.get_hours_list() if self.group_type == 'hour' else self.get_days_range()
        dataset, labels = [], []
        for value in date_range:
            if self.group_type != 'hour':
                date_str = str(value.timetuple().tm_yday) if self.group_type == 'day' else str(value.timetuple().tm_mon)
                labels.append(value.isoformat())
            else:
                date_str = str(value)
            cumulative_value = agg_result.get(date_str, 0)
            dataset.append(cumulative_value)
        if not labels:
            labels = [date.isoformat() for date in self.get_hours_range()]
        result = {'dataset': dataset, 'labels': labels}
        return result


