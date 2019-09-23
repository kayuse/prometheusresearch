from abc import ABC, abstractmethod
import psycopg2


class BaseModel(ABC):
    def __init__(self, db):
        self.db = db

    @abstractmethod
    def insert(self, data):
        pass

    @abstractmethod
    def query(self, query):
        pass

    @abstractmethod
    def get(self, data):
        pass

    @abstractmethod
    def update(self, data):
        pass

    def delete(self, data):
        pass


class Patient(BaseModel):

    def __init__(self, db):
        super().__init__(db=db)

    def insert(self, data):
        try:
            query = """INSERT INTO patient(source_id, birth_date, gender, race_code, race_code_system, ethnicity_code, 
            ethnicity_code_system, country) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"""
            insert_data = (data.get('source_id'), data.get('birth_date'), data.get('gender'), data.get('race_code'),
                           data.get('race_code_system'), data.get('ethnicity_code'), data.get('ethnicity_code_system'),
                           data.get('country'))
            self.db.cursor.execute(query, insert_data)
            self.db.connection.commit()
            return self.db.cursor.rowcount

        except (Exception, psycopg2.Error) as error:
            if self.db.connection:
                print("Failed to insert record into patient table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        try:

            query = """SELECT * FROM patient WHERE %fieldname = %s """
            fieldname = data['fieldname']
            query = query.replace('%fieldname', fieldname)
            query_data = (data['value'],)

            self.db.cursor.execute(query, query_data)
            row = self.db.cursor.fetchone()

            return row

        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            return None

    def update(self, data):
        pass

    def delete(self, data):
        pass


class Encounter(BaseModel):

    def __init__(self, db):
        super().__init__(db=db)

    def insert(self, data):
        try:
            query = """INSERT INTO encounter(source_id, patient_id, start_date,end_date,type_code,type_code_system) 
        VALUES(%s, %s, %s, %s, %s, %s)"""
            insert_data = (data.get('source_id'), data.get('patient_id'), data.get('start_date'), data.get('end_date'),
                           data.get('type_code'), data.get('type_code_system'))
            self.db.cursor.execute(query, insert_data)
            self.db.connection.commit()
            return self.db.cursor.rowcount

        except (Exception, psycopg2.Error) as error:
            if self.db.connection:
                print("Failed to insert record into encounter table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        try:
            query = """SELECT * FROM encounter WHERE %fieldname = %s """
            fieldname = data['fieldname']
            query = query.replace('%fieldname', fieldname)
            query_data = (data['value'],)

            self.db.cursor.execute(query, query_data)
            row = self.db.cursor.fetchone()

            return row

        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            return None

    def update(self, data):
        pass

    def delete(self, data):
        pass


class Procedure(BaseModel):

    def __init__(self, db):
        super().__init__(db=db)

    def insert(self, data):
        print('im here')
        try:
            query = """INSERT INTO procedure(source_id, patient_id, encounter_id, procedure_date, type_code,
             type_code_system) 
            VALUES(%s, %s, %s, %s, %s, %s)"""
            insert_data = (
                data.get('source_id'), data.get('patient_id'), data.get('encounter_id'), data.get('procedure_date'),
                data.get('type_code'), data.get('type_code_system'))
            self.db.cursor.execute(query, insert_data)
            self.db.connection.commit()
            return self.db.cursor.rowcount

        except (Exception, psycopg2.Error) as error:

            if self.db.connection:
                self.db.connection.rollback()
                print("Failed to insert record into mobile table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        pass

    def update(self, data):
        pass

    def delete(self, data):
        pass


class Observation(BaseModel):

    def __init__(self, db):
        super().__init__(db=db)

    def insert(self, data):
        try:
            query = """INSERT INTO observation(source_id, patient_id, encounter_id, observation_date, type_code, 
            type_code_system, value, unit_code, unit_code_system) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s,%s)"""
            insert_data = (
                data.get('source_id'), data.get('patient_id'), data.get('encounter_id'), data.get('observation_date'),
                data.get('type_code'), data.get('type_code_system'), data.get('value'),
                data.get('unit_code'), data.get('unit_code_system'))
            self.db.cursor.execute(query, insert_data)
            self.db.connection.commit()
            return self.db.cursor.rowcount

        except (Exception, psycopg2.Error) as error:
            if self.db.connection:
                print("Failed to insert record into mobile table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        pass

    def update(self, data):
        pass

    def delete(self, data):
        pass
