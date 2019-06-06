import os
from data_ingestion.file_processor import xml_processor

processor = xml_processor.XMLProcessor(
            file_path=('reduced_4.xml'),
            rowtag='Positions'
        )

processor.set_processor_arrays(['1'],['1'],['1'],['1'],['1'],['1'])
processor.process()