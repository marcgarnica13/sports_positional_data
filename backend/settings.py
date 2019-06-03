schema_lookup = {
        'url': 'regex("[\w]+")',
        'field': 'schema_identifier'
}

basic_schema = {
    'schema_identifier': {
        'type': 'string'
    }
}

mappings = {
    'item_title': 'Mapping',
    'allow_unknown': True
}

teams = {
    'item_title': 'Team',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup,
    'schema': basic_schema
}

participants = {
    'item_title': 'Participant',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup,
    'schema': basic_schema
}

games = {
    'item_title': 'Game',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup,
    'schema': basic_schema
}

gameSections = {
    'item_title': 'Game section',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup,
    'schema': basic_schema
}

moments = {
    'item_title': 'Moment',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup,
    'schema': basic_schema
}

DOMAIN = {
    'mappings': mappings,
    'Games': games,
    'GameSections': gameSections,
    'Participants': participants,
    'Teams': teams
}

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'test_positional_data'

RESOURCE_METHODS = ['GET', 'POST', 'DELETE']
ITEM_METHODS = ['GET', 'PATCH', 'PUT', 'DELETE']
