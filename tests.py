import unittest
import ndjson, requests
from util.patient_manager import FHIRPatientResourcesManager
from util.encounter_manager import FHIREncounterResourceManager


class TestPatient(unittest.TestCase):
    def test_patient_process(self):
        patient = FHIRPatientResourcesManager()
        with open('test/patient.ndjson') as f:
            patient_data = ndjson.load(f)

        data = patient.run_patient(patient_data[0])

        correct_data = {
            'source_id': 'e2309dce-a235-4624-af3c-ad209843fe93',
            'birth_date': '1961-12-24',
            'gender': 'female',
            'country': 'US',
            'race_code': '2106-3',
            'race_code_system': 'http://hl7.org/fhir/v3/Race',
            'ethnicity_code': '2186-5',
            'ethnicity_code_system': 'http://hl7.org/fhir/v3/Ethnicity',
        }

        self.assertDictEqual(correct_data, data)

    def test_encounter_process(self):
        encounter = FHIREncounterResourceManager()
        with open('test/encounter.ndjson') as f:
            encounter_data = ndjson.load(f)

        data = encounter.run_encounter_process(encounter_data[0])

        self.assertNotEqual(data['source_id'], None)
        self.assertNotEqual(data['patient'], None)
        self.assertNotEqual(data['start_date'], None)
        self.assertNotEqual(data['end_date'], None)

    def test_procedure_process(self):
        encounter = FHIREncounterResourceManager()
        with open('test/encounter.ndjson') as f:
            encounter_data = ndjson.load(f)

        data = encounter.run_encounter_process(encounter_data[0])

        self.assertNotEqual(data['source_id'], None)
        self.assertNotEqual(data['patient'], None)
        self.assertNotEqual(data['start_date'], None)
        self.assertNotEqual(data['end_date'], None)


if __name__ == '__main__':
    unittest.main()
