from mongoengine import EmbeddedDocument


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
                self.pipelines.append(arg.to_dict())
            else:
                raise ValueError(f"Invalid Q object")
        else:
            self.q = kwargs
            self.to_aggregate()
            self.pipelines = self.combine_sequential_matches()
        return self

    @staticmethod
    def match(matches):
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
            'icontains': '$regex',
            'startswith': '$regex',
            'istartswith': '$regex',
            'endswith': '$regex',
            'iendswith': '$regex',
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
                if (operator := key.rsplit("__", 1)[1]) not in list(mongo_comparison_operators.keys()):
                    raise ValueError(f"Invalid operator: {operator}")
            if '__' not in key:
                match_query[key] = value
                continue

            field, operator = key.rsplit('__', 1)
            if operator not in mongo_operators:
                raise ValueError(f"Unsupported operator: {operator}")

            if operator in ['exact', 'iexact']:
                match_query[field] = {mongo_operators[operator]: value}
            elif operator in ['contains', 'startswith', 'endswith', 'icontains', 'istartswith', 'iendswith']:
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
            if key in skip_list: continue
            split_query = key.split('__')
            join_field = self.base_model._fields.get(split_query[0])
            if not join_field:
                raise ValueError(f"Invalid field: {split_query[0]}")
            # This is a nested query.
            if 'document_type_obj' not in join_field.__dict__ or issubclass(join_field.document_type, EmbeddedDocument):
                self.pipelines.append(self.match([(key, value)]))
            else:
                from_collection = join_field.document_type._meta['collection']
                local_field = join_field.db_field
                as_name = join_field.name
                matches = []
                for key, value in self.q.items():
                    if key.split('__')[0] == split_query[0]:
                        key = local_field
                        skip_list.append(key)
                        matches.append((key.replace('__', '.'), value))
                self.pipelines.extend([
                    self.lookup(
                        from_collection=from_collection,
                        local_field=local_field,
                        as_name=as_name
                    ),
                    self.unwind(as_name),
                    self.match(matches)
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

    def group(self, **kwargs):
        group_dict = {}
        for k, v in kwargs.items():
            group_dict[k] = v
        self.pipelines.append({'$group': group_dict})
        return self

    def order_by(self, field):
        self.pipelines.append({'$sort': {
            f'{field.replace("-", "")}': -1 if field.startswith('-') else 1}
        })
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
        self.conditions = Aggify.match(conditions.items()).get('$match')

    def to_dict(self):
        return {"$match": self.conditions}

    def __or__(self, other):
        combined_conditions = {"$or": [self.conditions, other.to_dict()["$match"]]}
        return Q(**combined_conditions)

    def __and__(self, other):
        combined_conditions = {"$and": [self.conditions, other.to_dict()["$match"]]}
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
        combined_field = {"$add": [self.field, other]}
        return F(combined_field)

    def __sub__(self, other):
        if isinstance(other, F):
            other = other.field
        combined_field = {"$subtract": [self.field, other]}
        return F(combined_field)

    def __mul__(self, other):
        if isinstance(other, F):
            other = other.field
        combined_field = {"$multiply": [self.field, other]}
        return F(combined_field)

    def __truediv__(self, other):
        if isinstance(other, F):
            other = other.field
        combined_field = {"$divide": [self.field, other]}
        return F(combined_field)
