from abc import ABC, abstractmethod
import ndjson, sys
import requests
from db.manager import BaseModel


class FHIRResourcesManager(ABC):
    base_url = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/'
    fhis_race_url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
    fhis_ethinicity_url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
    model = BaseModel

    def __init__(self, db):
        self.db = db

    @abstractmethod
    def run(self):
        self.fetch()

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            self.process(items)

    @abstractmethod
    def process(self, data):
        pass

    @abstractmethod
    def store(self, data):
        pass
