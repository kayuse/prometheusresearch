from .base_manager import FHIRResourcesManager
from db.manager import Patient
import ndjson, sys
import requests


class FHIRPatientResourcesManager(FHIRResourcesManager):

    def __init__(self, db):
        super().__init__(db=db)
        self.model = Patient(db=self.db)
        self.base_url = self.base_url + 'Patient.ndjson'

    def fetch(self):
        super().fetch()

    def run(self):
        super().run()

    def process(self, patients):
        for patient in patients:
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
            print(data)

            self.store(data)

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

    def store(self, data):
        id = self.model.insert(data=data)
        return id
