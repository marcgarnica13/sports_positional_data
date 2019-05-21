mapping = {
    'name' : 'testMapping',
    #CSV, JSON, XML, AVRO, EXCEL, OWL
    'file': {
        'format': 'csv',
        'delimiter': ';',
        'header': 0
    },
    'data_point' : {
        'source_name' : {
            'a' : 'feature',
            'value':'dshs#input',
            'description': 'Source name'
        },
        'source_identifier' : {
            'a': 'feature',
            'value':'dshs#input',
            'description': 'Source unique identifier'
        },
        'Games': {
            'a': 'collection',
            'schema_identifier': 'dshs#mapping_1',
        },
        'Participants': {
            'a': 'collection',
            'schema_identifier': 'sensor id',
            'shirt_number': 'number',
            'full_name': 'full name',
            'team_id': 'group id',
        },
        'Teams': {
            'a': 'collection',
            'schema_identifier': 'group id',
            'name': 'group name'
        },
        'GameSections': {
            'a': 'collection',
            'schema_identifier': 'dshs#mapping_1',
            'game_id' : 'dshs#mapping_1',
            'moments': {
                'a' : 'nested_collection',
                'schema_identifier': 'ts in ms',
                'timestamp': 'ts in ms',
                'participant': {
                    'a': 'nested_array',
                    'schema_identifier': 'sensor id',
                    'coord_x': 'x in m',
                    'coord_y': 'y in m',
                    'coord_z': 'z in m',
                    'additional_fields' : [
                        'speed in m/s', 'direction of movement in deg', 'acceleration in m/s2', 'total distance in m', 'metabolic power in W/kg', 'acceleration load'
                    ]
                }
            }
        }
    }
}