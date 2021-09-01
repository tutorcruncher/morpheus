from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Query, Session
from typing import TYPE_CHECKING, List, Union

from src.schema import SendMethod

if TYPE_CHECKING:
    from src.models import Company, Event, Link, Message, MessageGroup

Models = Union['Company', 'Event', 'Message', 'MessageGroup', 'Link']


agg_sql = """
select json_build_object(
  'histogram', histogram,
  'all_90_day', coalesce(agg.all_90, 0),
  'open_90_day', coalesce(agg.open_90, 0),
  'all_28_day', coalesce(agg.all_28, 0),
  'open_28_day', coalesce(agg.open_28, 0),
  'all_7_day', coalesce(agg.all_7, 0),
  'open_7_day', coalesce(agg.open_7, 0)
)
from (
  select coalesce(json_agg(t), '[]') AS histogram from (
    select coalesce(sum(count), 0) as count, date as day, status
    from message_aggregation
    where company_id = %(company_id)s and method = '%(method)s' and date > current_timestamp::date - '28 days'::interval
    group by date, status
  ) as t
) as histogram,
(
  select
    sum(count) as all_90,
    sum(count) filter (where status = 'open') as open_90,
    sum(count) filter (where date > current_timestamp::date - '28 days'::interval) as all_28,
    sum(count) filter (where date > current_timestamp::date - '28 days'::interval and status = 'open') as open_28,
    sum(count) filter (where date > current_timestamp::date - '7 days'::interval) as all_7,
    sum(count) filter (where date > current_timestamp::date - '7 days'::interval and status = 'open') as open_7
  from message_aggregation
  where company_id = %(company_id)s and method = '%(method)s'
) as agg
"""


def get_messages_aggregated(conn, company_id, method: SendMethod):
    data = conn.execute(agg_sql % {'company_id': company_id, 'method': method.name})
    return data.scalar()


class BaseManager:
    """
    A query handler for the more basic queries, rather than having repeated code in
    """

    model: Union[Models] = None

    def __init__(self, model=None):
        if not self.model:
            self.model = model

    def __call__(self, db: Session):
        self.db = db
        return self

    def count(self, *args, **kwargs) -> int:
        return self.db.query(self.model).filter(*args).filter_by(**kwargs).count()

    def get(self, **kwargs) -> Union[Models]:
        return self.db.query(self.model).filter_by(**kwargs).one()

    def filter(self, *args, **kwargs) -> Query:
        q = self.db.query(self.model)
        if args:
            q = q.filter(*args)
        if kwargs:
            q = q.filter_by(**kwargs)
        return q

    def all(self) -> List[Union[Models]]:
        return self.db.query(self.model).all()

    def create(self, **kwargs) -> Union[Models]:
        instance = self.model(**kwargs)
        self.db.add(instance)
        self.db.commit()
        self.db.flush()
        return instance

    def create_many(self, *instances: List[Union[Models]]) -> None:
        self.db.add_all(*[instances])
        self.db.commit()

    def get_or_create(self, **kwargs) -> Union[Models]:
        try:
            instance = self.get(**kwargs)
        except NoResultFound:
            instance = self.create(**kwargs)
        return instance

    def update(self, instance: Union[Models]):
        assert instance.id
        self.db.add(instance)
        self.db.commit()
        self.db.flush()
        return instance

    def delete(self, *args, **kwargs) -> int:
        count = self.db.query(self.model).filter(*args).filter_by(**kwargs).delete()
        self.db.commit()
        self.db.flush()
        return count
