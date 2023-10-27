from mongoengine import EmbeddedDocument, EmbeddedDocumentField


class Aggify:

    def __init__(self, base_model):
        """
        Initializes the Aggify class.

        Args:
            base_model: The base model class.
        """
        self.base_model = base_model
        self.pipelines = []
        self.start = None
        self.stop = None
        self.q = None

    def __getitem__(self, index):
        if isinstance(index, slice):
            # If a slice is provided (e.g., [0:10]), apply offset and limit
            self.start = index.start
            self.stop = index.stop
        elif isinstance(index, int):
            # If an integer is provided, return a single item by its index
            self.start = 0
            self.stop = index
        else:
            raise ValueError("Invalid index type")

        if self.start:
            self.pipelines.append({'$skip': self.start})

        if self.stop:
            self.pipelines.append({'$limit': int(self.stop - self.start)})

        return self

    def filter(self, arg=None, **kwargs):
        if arg:
            if isinstance(arg, Q):
                self.pipelines.append(dict(arg))
            else:
                raise ValueError(f"Invalid Q object")
        else:
            self.q = kwargs
            self.to_aggregate()
            self.pipelines = self.combine_sequential_matches()
        return self

    def match(self, matches):
        """
        Generates a MongoDB match pipeline stage.

        Args:
            matches: The match criteria.

        Returns:
            A MongoDB match pipeline stage.
        """
        mongo_operators = {
            'exact': '$eq',
            'iexact': '$eq',
            'contains': '$regex',
            'icontains': '$regex',  # noqa
            'startswith': '$regex',
            'istartswith': '$regex',  # noqa
            'endswith': '$regex',
            'iendswith': '$regex',  # noqa
            'in': "$in",
            'ne': "$ne",
            'not': "$not",
        }

        mongo_comparison_operators = {
            'lt': '$lt',
            'lte': '$lte',
            'gt': '$gt',
            'gte': '$gte',
        }

        mongo_operators |= mongo_comparison_operators

        match_query = {}
        for match in matches:
            key, value = match
            if isinstance(value, F):
                if '__' not in key:
                    raise ValueError("You should use comparison operators with F function")
                if (operator := key.rsplit("__", 1)[1]) not in mongo_comparison_operators:
                    raise ValueError(f"Invalid operator: {operator}")
            if '__' not in key:
                match_query[key] = value
                continue
            field, operator, *_ = key.split('__')
            if self.base_model and isinstance(self.base_model._fields.get(field), EmbeddedDocumentField):  # noqa
                self.pipelines.append(self.match([(key.replace("__", ".", 1), value)]))
                continue
            if operator not in mongo_operators:
                raise ValueError(f"Unsupported operator: {operator}")

            if operator in ['exact', 'iexact']:
                match_query[field] = {mongo_operators[operator]: value}
            elif operator in ['contains', 'startswith', 'endswith', 'icontains', 'istartswith', 'iendswith']:  # noqa
                match_query[field] = {mongo_operators[operator]: f".*{value}.*", '$options': 'i'}
            elif operator in mongo_comparison_operators:
                if isinstance(value, F):
                    match_query['$expr'] = {mongo_operators[operator]: [f"${field}", value.to_dict()]}
                else:
                    match_query[field] = {mongo_operators[operator]: value}
            else:
                match_query[field] = {mongo_operators[operator]: value}

        return {"$match": match_query}

    @staticmethod
    def lookup(from_collection, local_field, as_name, foreign_field='_id'):
        """
        Generates a MongoDB lookup pipeline stage.

        Args:
            from_collection: The name of the collection to lookup.
            local_field: The local field to join on.
            as_name: The name of the new field to create.
            foreign_field: The foreign field to join on.

        Returns:
            A MongoDB lookup pipeline stage.
        """
        return {
            '$lookup': {
                'from': from_collection,
                'localField': local_field,
                'foreignField': foreign_field,
                'as': as_name
            }
        }

    @staticmethod
    def unwind(path, preserve=True):
        """
        Generates a MongoDB unwind pipeline stage.

        Args:
            path: The path to unwind.
            preserve: Whether to preserve null and empty arrays.

        Returns:
            A MongoDB unwind pipeline stage.
        """
        return {
            '$unwind': {
                'path': f'${path}',
                'preserveNullAndEmptyArrays': preserve
            }
        }

    def to_aggregate(self):
        """
        Builds the pipelines list based on the query parameters.
        """
        skip_list = []
        for key, value in self.q.items():
            if key in skip_list:
                continue
            split_query = key.split('__')
            join_field = self.base_model._fields.get(split_query[0])  # noqa
            if not join_field:
                raise ValueError(f"Invalid field: {split_query[0]}")
            # This is a nested query.
            if 'document_type_obj' not in join_field.__dict__ or issubclass(join_field.document_type, EmbeddedDocument):
                match = self.match([(key, value)])
                if (match.get("$match")) != {}:
                    self.pipelines.append(match)
            else:
                from_collection = join_field.document_type._meta['collection']  # noqa
                local_field = join_field.db_field
                as_name = join_field.name
                matches = []
                for k, v in self.q.items():
                    if k.split('__')[0] == split_query[0]:
                        skip_list.append(k)
                        if (match := self.match([(k.replace("__", ".", 1), v)]).get("$match")) != {}:
                            matches.append(match)
                self.pipelines.extend([
                    self.lookup(
                        from_collection=from_collection,
                        local_field=local_field,
                        as_name=as_name
                    ),
                    self.unwind(as_name),
                    *[{"$match": match} for match in matches]
                ])

    def combine_sequential_matches(self):
        merged_pipeline = []
        match_stage = None

        for stage in self.pipelines:
            if stage.get('$match'):
                if match_stage is None:
                    match_stage = stage['$match']
                else:
                    match_stage.update(stage['$match'])
            else:
                if match_stage:
                    merged_pipeline.append({'$match': match_stage})
                    match_stage = None
                merged_pipeline.append(stage)

        if match_stage:
            merged_pipeline.append({'$match': match_stage})

        return merged_pipeline

    def project(self, **kwargs):
        projects = {}
        for k, v in kwargs.items():
            projects[k] = v
        self.pipelines.append(
            {"$project": projects}
        )
        return self

    def group(self, key="_id"):
        self.pipelines.append({'$group': {"_id": f"${key}"}})
        return self

    def annotate(self, annotate_name, accumulator, f):
        if (stage := list(self.pipelines[-1].keys())[0]) != "$group":
            raise ValueError(f"Annotations apply only to $group, not to {stage}.")

        accumulator_dict = {
            "sum": "$sum",
            "avg": "$avg",
            "first": "$first",
            "last": "$last",
            "max": "$max",
            "min": "$min",
            "push": "$push",
            "addToSet": "$addToSet",
            "stdDevPop": "$stdDevPop",
            "stdDevSamp": "$stdDevSamp"  # noqa
        }

        acc = accumulator_dict.get(accumulator, None)
        if not acc:
            raise ValueError(f"Invalid accumulator: {accumulator}")

        if isinstance(f, F):
            value = f.to_dict()
        else:
            value = f"${f}"
        self.pipelines[-1]['$group'] |= {annotate_name: {acc: value}}

    def order_by(self, field):
        self.pipelines.append({'$sort': {
            f'{field.replace("-", "")}': -1 if field.startswith('-') else 1}
        })
        return self

    def raw(self, raw_query):
        self.pipelines.append(raw_query)
        return self

    def aggregate(self):
        """
        Returns the aggregated results.

        Returns:
            The aggregated results.
        """
        return self.base_model.objects.aggregate(*self.pipelines)


class Q:
    def __init__(self, **conditions):
        self.conditions = Aggify(None).match(matches=conditions.items()).get('$match')

    def __iter__(self):
        yield '$match', self.conditions

    def __or__(self, other):
        if self.conditions.get("$or", None):
            self.conditions["$or"].append(dict(other)["$match"])
            combined_conditions = self.conditions
        else:
            combined_conditions = {"$or": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __and__(self, other):
        if self.conditions.get("$and", None):
            self.conditions["$and"].append(dict(other)["$match"])
            combined_conditions = self.conditions
        else:
            combined_conditions = {"$and": [self.conditions, dict(other)["$match"]]}
        return Q(**combined_conditions)

    def __invert__(self):
        combined_conditions = {"$not": [self.conditions]}
        return Q(**combined_conditions)


class F:
    def __init__(self, field):
        if isinstance(field, str):
            self.field = f"${field}"
        else:
            self.field = field

    def to_dict(self):
        return self.field

    def __add__(self, other):
        if isinstance(other, F):
            other = other.field

        if type(self.field) == dict and self.field.get("$add", None):
            self.field["$add"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$add": [self.field, other]}
        return F(combined_field)

    def __sub__(self, other):
        if isinstance(other, F):
            other = other.field

        if type(self.field) == dict and self.field.get("$subtract", None):
            self.field["$subtract"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$subtract": [self.field, other]}
        return F(combined_field)

    def __mul__(self, other):
        if isinstance(other, F):
            other = other.field

        if type(self.field) == dict and self.field.get("$multiply", None):
            self.field["$multiply"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$multiply": [self.field, other]}
        return F(combined_field)

    def __truediv__(self, other):
        if isinstance(other, F):
            other = other.field

        if type(self.field) == dict and self.field.get("$divide", None):
            self.field["$divide"].append(other)
            combined_field = self.field
        else:
            combined_field = {"$divide": [self.field, other]}
        return F(combined_field)


class Cond:
    """
    input: Cond(23, '>', 20, 'hi', 'bye')
    return: {'$cond': {'if': {'$gt': [23, 20]}, 'then': 'hi', 'else': 'bye'}}
    """
    OPERATOR_MAPPING = {
        '>': '$gt',
        '>=': '$gte',
        '<': '$lt',
        '<=': '$lte',
        '==': '$eq',
        '!=': '$ne'
    }

    def __init__(self, value1, condition, value2, then_value, else_value):
        self.value1 = value1
        self.value2 = value2
        self.condition = self._map_condition(condition)
        self.then_value = then_value
        self.else_value = else_value

    def _map_condition(self, condition):
        if condition in self.OPERATOR_MAPPING:
            return self.OPERATOR_MAPPING[condition]
        raise ValueError("Unsupported operator")

    def __iter__(self):
        """Iterator used by `dict` to create a dictionary from a `Cond` object

        With this method we are now able to do this:
        c = Cond(...)
        dict_of_c = dict(c)

        instead of c.to_dict()

        Returns:
            A tuple of '$cond' and its value
        """
        yield (
            "$cond", {
                "if": {self.condition: [self.value1, self.value2]},
                "then": self.then_value,
                "else": self.else_value
            }
        )
