default_mapping = {
    'name': 'default_mapping',
    # CSV, JSON, XML, AVRO, EXCEL, OWL
    'file': {
    },
    'data_point': {
        'source_name': {
            'a': 'feature',
            'value': '',
            'description': ''
        },

        'source_identifier': {
            'a': 'feature',
            'value': '',
            'description': ''
        },

        'Games': {
            'a': 'collection',
            'schema_identifier': '',
        },

        'Participants': {
            'a': 'collection',
            'schema_identifier': '',
            'shirt_number': '',
            'full_name': '',
            'team_id': '',
        },
        'Teams': {
            'a': 'collection',
            'schema_identifier': '',
            'name': ''
        },
        'GameSections': {
            'a': 'collection',
            'schema_identifier': '',
            'game_id': '',
            'moments': {
                'a': 'nested_collection',
                'schema_identifier': '',
                'timestamp': '',
                'participant': {
                    'a': 'nested_array',
                    'schema_identifier': '',
                    'coord_x': '',
                    'coord_y': '',
                    'coord_z': '',
                    'additional_fields': []
                }
            }
        }
    }
}

kinexon_mapping = {
    'name': 'kinexon_mapping',
    # CSV, JSON, XML, AVRO, EXCEL, OWL
    'file': {
        'format': 'csv',
        'delimiter': ';',
        'header': 'True',
        'timestamp_format': ''
    },
    'data': {
        'source_name': {
            'a': 'feature',
            'value': 'dshs#input',
            'description': 'Source name'
        },

        'source_identifier': {
            'a': 'feature',
            'value': 'dshs#input',
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
            'game_id': 'dshs#mapping_1',
            'moments': {
                'a': 'nested_collection',
                'join_key': 'section_id',
                'schema_identifier': 'ts in ms',
                'timestamp': 'ts in ms',
                'section_id': 'dshs#mapping_1',
                'participants': {
                    'a': 'nested_array',
                    'id': 'sensor id',
                    'coord_x': 'x in m',
                    'coord_y': 'y in m',
                    'coord_z': 'z in m',
                    'additional_fields': [
                        'speed in m/s', 'direction of movement in deg', 'acceleration in m/s2', 'total distance in m',
                        'metabolic power in W/kg', 'acceleration load'
                    ]
                }
            }
        }
    }
}

dfl_mapping = {
    'name': 'default_mapping',
    # CSV, JSON, XML, AVRO, EXCEL, OWL
    'file': {
        'format':'xml',
        'rowTag': 'Positions'
    },
    'data': {
        'source_name': {
            'a': 'feature',
            'value': 'dshs#input',
            'description': 'Source name'
        },

        'source_identifier': {
            'a': 'feature',
            'value': 'dshs#input',
            'description': 'Source unique identifier'
        },

        'GameSections': {
            'a': 'collection',
            'schema_identifier': 'FrameSet/_GameSection',
            'game_id': 'FrameSet/_MatchId',
            'moments': {
                'a': 'nested_collection',
                'join_key': 'section_id',
                'schema_identifier': 'FrameSet/Frame/_N',
                'timestamp': 'FrameSet/Frame/_T',
                'section_id': 'FrameSet/_GameSection',
                'participants': {
                    'a': 'nested_array',
                    'id': 'FrameSet/_PersonId',
                    'coord_x': 'FrameSet/Frame/_X',
                    'coord_y': 'FrameSet/Frame/_Y',
                    'coord_z': 'FrameSet/Frame/_Z',
                    'additional_fields': [
                        'FrameSet/Frame/_S', 'FrameSet/Frame/_M',
                    ]
                }
            }
        }

    }
}

'''

        'Games': {
            'a': 'collection',
            'schema_identifier': 'Metadata._MatchId',
            "additional_fields": [
                "Metadata.PitchSize._X", "Metadata.PitchSize._Y"
                ]
        },

        'Teams': {
            'a': 'collection',
            'schema_identifier': 'FrameSet/_TeamId',
        },

        'Participants': {
            'a': 'collection',
            'schema_identifier': 'FrameSet/_PersonId',
            'team_id': 'FrameSet/_TeamId',
        },'''

