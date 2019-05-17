mappings = {
    'item_title': 'Mapping',
    'allow_unknown': True
}

teams = {
    'item_title': 'Team',
    'allow_unknown': True
}

participants = {
    'item_title': 'Participant',
    'allow_unknown': True
}

games = {
    'item_title': 'Game',
    'allow_unknown': True
}

gameSections = {
    'item_title': 'Game section',
    'allow_unknown': True
}

DOMAIN = {
    'mappings': mappings,

}

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

MONGO_DBNAME = 'test_positional_data'

RESOURCE_METHODS = ['GET', 'POST', 'DELETE']
ITEM_METHODS = ['GET', 'PATCH', 'PUT', 'DELETE']
