"""Aggify - an async-first MongoDB ODM with a Django-style API.

import aggify
from aggify import Document, StringField, IntField, Q, F

aggify.connect("mongodb://localhost:27017/shop")

class Product(Document):
    name = StringField(required=True)
    price = IntField()

await Product.objects.acreate(name="pen", price=3)
cheap = await Product.objects.filter(price__lt=10).alist()
"""

from aggify.conf import configure, settings
from aggify.connection import (
    DEFAULT_ALIAS,
    aclose_clients,
    adisconnect,
    aping,
    atransaction,
    connect,
    current_session,
    disconnect,
    get_async_client,
    get_async_collection,
    get_async_database,
    get_client,
    get_collection,
    get_config,
    get_database,
    is_connected,
    ping,
    transaction,
)
from aggify.documents import BaseDocument, Document, EmbeddedDocument, Manager, Options
from aggify.exceptions import (
    AggifyBaseException,
    AggifyError,
    AggifyValueError,
    AlreadyExistsField,
    AnnotationError,
    ConfigurationError,
    ConnectionNotConfigured,
    InvalidArgument,
    InvalidEmbeddedField,
    InvalidField,
    InvalidLookup,
    InvalidOperator,
    InvalidPipelineStageError,
    InvalidProjection,
    MongoIndexError,
    MultipleObjectsReturned,
    NotUniqueError,
    ObjectDoesNotExist,
    OptimisticLockError,
    OutStageError,
    PipelineError,
    QueryError,
    SecurityError,
    SynchronousOnlyOperation,
    UnsafeQueryError,
    ValidationError,
)
from aggify.expressions import *  # noqa: F401,F403
from aggify.expressions import (  # explicit for linters
    AddToSet,
    Avg,
    Case,
    Cond,
    Count,
    DatePart,
    Expression,
    F,
    First,
    Func,
    Last,
    Max,
    Min,
    Push,
    Q,
    Sum,
    Value,
    Var,
    When,
)
from aggify.fields import *  # noqa: F401,F403
from aggify.indexes import Index
from aggify.pipeline import Aggify, Pipeline
from aggify.queryset import QuerySet
from aggify.security import Raw
from aggify.signals import (
    Signal,
    post_delete,
    post_init,
    post_save,
    pre_delete,
    pre_init,
    pre_save,
    receiver,
)

__version__ = "1.0.0"
