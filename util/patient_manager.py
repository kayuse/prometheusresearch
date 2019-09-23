from .base_manager import FHIRResourcesManager
from db.manager import Patient
from db import DB
import ndjson, sys
import requests, math

from multiprocessing import Process, Pool


class FHIRPatientResourcesManager(FHIRResourcesManager):
    pool_count = 2

    def __init__(self):
        super().__init__()
        # self.model = Patient(db=self.db)
        self.base_url = self.base_url + 'Patient.ndjson'

    def fetch(self):
        print('About to begin fetching from ' + self.base_url)
        with requests.get(self.base_url, stream=True) as r:
            print('Request successful')
            items = r.json(cls=ndjson.Decoder)
            total_items = len(items)

            item_cursor = 0
            batch_count = math.ceil(total_items / self.pool_count)
            to_item = batch_count
            processes = []
            for i in range(self.pool_count):
                current_item = items[item_cursor:to_item]
                item_cursor += batch_count
                to_item += batch_count
                p = Process(target=self.process, args=(current_item,))
                processes.append(p)

            [x.start() for x in processes]
            [x.join() for x in processes]

    def run(self):
        self.fetch()

    def process(self, patients):
        db = DB()
        db.connect()
        for patient in patients:
            data = self.run_patient(patient)
            self.store(data, db)
        db.close()

    def run_patient(self, patient):
        source_id = patient.get('id', None)
        birth_date = patient.get('birthDate', None)
        gender = patient.get('gender', None)
        country = self.get_country(patient)
        extensions = self.get_extensions(patient=patient)
        data = {
            'source_id': source_id,
            'birth_date': birth_date,
            'gender': gender,
            'country': country,
            'race_code': extensions['race'].get('code'),
            'race_code_system': extensions['race'].get('system'),
            'ethnicity_code': extensions['ethnicity'].get('code'),
            'ethnicity_code_system': extensions['ethnicity'].get('system'),
        }
        return data

    def get_country(self, patient):
        address = patient.get('address', None)

        if address is not None and isinstance(address, list):
            return address[0].get('country', None)
        return None

    def get_extension_data(self, extension):

        value_codeable_concept = extension.get('valueCodeableConcept')
        if value_codeable_concept is None:
            return None
        if value_codeable_concept.get('coding') is None or not isinstance(value_codeable_concept.get('coding'), list):
            return None
        code = value_codeable_concept.get('coding')[0].get('code')
        system = value_codeable_concept.get('coding')[0].get('system')
        return code, system

    def get_extensions(self, patient):
        extensions = patient.get('extension', None)
        extracted_extensions = {'race': {'code': None, 'system': None}, 'ethnicity': {'code': None, 'system': None}}
        if extensions is None or not isinstance(extensions, list):
            return extracted_extensions
        for extension in extensions:
            if extension['url'] == self.fhis_race_url:
                code, system = self.get_extension_data(extension)
                extracted_extensions["race"] = {'code': code, 'system': system}
            if extension['url'] == self.fhis_ethinicity_url:
                code, system = self.get_extension_data(extension)
                extracted_extensions["ethnicity"] = {'code': code, 'system': system}

        return extracted_extensions

    def store(self, data, db):
        self.model = Patient(db=db)
        id = self.model.insert(data=data)
        return id
