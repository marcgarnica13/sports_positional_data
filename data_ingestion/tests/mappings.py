mapping = {
    'name' : 'testMapping',
    #CSV, JSON, XML, AVRO, EXCEL, OWL
    'file': {
        'format': 'csv',
        'delimiter': ';',
        'header': 0
    },
    'data_point' : {
        'source_name' : 'dshs#input',
        'source_identifier' : 'dshs#input',
        'Game': {
            'schema_identifier' : 'dshs#input',
        },
        'Participant': {
            'schema_identifier': 'sensor id',
            'shirt_number': 'number',
            'full_name': 'full name',
            'team_id': 'group id',
        },
        'Team': {
            'schema_identifier': 'group id',
            'name': 'group name'
        },
        'GameSection': {
            'schema_identifier': 'dshs#input',
        },
        'Moment': {
            'timestamp': 'ts in ms',
            'coord_x': 'x in m',
            'coord_y': 'y in m',
            'coord_z': 'z in m',
            'time_units': 'ms',
            'coord_units': 'm',
            'additional_fields' : [
                'speed in m/s', 'direction of movement in deg', 'acceleration in m/s2', 'total distance in m', 'metabolic power in W/kg', 'acceleration load'
            ]
        }
    }

}