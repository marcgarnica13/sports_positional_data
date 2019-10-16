import os
from data_ingestion.file_processor import xml_processor, csv_processor

processor = xml_processor.XMLProcessor(
    file_path='/home/marc/Development/sports_analytics/game_changer/real_sociedad_kickoff/data_examples/tournaments.xml',
    rowtag="data",
    value_tag="_xml_value",
    time_format="ms")

