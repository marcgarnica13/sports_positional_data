schema_lookup_domain = {
        'url': 'regex("[\w]+")',
        'field': 'schema_identifier'
}

schema_lookup_mappings = {
    'url': 'regex("[\w]+")',
    'field': 'name'
}

basic_schema_mappings = {
    'name': {
        'type': 'string'
    }
}

basic_schema = {
    'schema_identifier': {
        'type': 'string'
    }
}

mappings = {
    'item_title': 'Mapping',
    'allow_unknown': True,
    'additional_lookup': schema_lookup_mappings,
    'schema': basic_schema_mappings
}

teams = {
    'item_title': 'Team',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup_domain,
    'schema': basic_schema
}

participants = {
    'item_title': 'Participant',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup_domain,
    'schema': basic_schema
}

games = {
    'item_title': 'Game',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup_domain,
    'schema': basic_schema
}

gameSections = {
    'item_title': 'Game section',
    'allow_unknown': True,
    'additional_lookup' : schema_lookup_domain,
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
