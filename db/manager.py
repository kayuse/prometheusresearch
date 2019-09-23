from abc import ABC, abstractmethod
import psycopg2, sys


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


class App(BaseModel):
    def __init__(self, db):
        super().__init__(db)

    def insert(self, data):
        try:
            query = """INSERT INTO app(name,started_at) 
        VALUES(%s, %s)"""
            insert_data = (data.get('name'), data.get('started_at'))
            self.db.cursor.execute(query, insert_data)
            self.db.cursor.execute('SELECT LASTVAL()')
            return self.db.cursor.fetchone()[0]

        except (Exception, psycopg2.Error) as error:
            if self.db.connection:
                print("Failed to insert record into app table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        pass

    def update(self, data):
        try:
            query = """UPDATE app set %set_fieldname = %s WHERE %get_fieldname = %s"""
            query = query.replace('%set_fieldname', data['set_fieldname']).replace('%get_fieldname', 'get_fieldname')
            self.db.cursor.execute(query, (data['set_value'], data['get_value']))
            self.db.connect.commit()
            return self.db.cursor.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            print("Error in update operation", error)
            return None


class Report(BaseModel):
    def __init__(self, db):
        super().__init__(db)

    def query(self, query):
        pass

    def get_top_procedure_types(self, limit):
        query = """SELECT type_code_system,type_code,count(type_code) as c 
            FROM public.procedure group by (type_code,type_code_system) order by c desc limit %limit"""
        query = query.replace("%limit", str(limit))
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchall()
        return row

    def no_of_patient_by_gender(self):
        query = """SELECT gender,count(gender) from patient GROUP BY gender"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchall()
        male = row[0]
        female = row[1]
        return male, female

    def count_patient(self):
        query = """SELECT count(*) as c FROM patient"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row[0]

    def count_encounter(self):
        query = """SELECT count(*) as c FROM encounter"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row[0]

    def count_procedure(self):
        query = """SELECT count(*) as c FROM procedure"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row[0]

    def count_observation(self):
        query = """SELECT count(*) as c FROM observation"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row[0]

    def get_most_popular_day(self):
        query = """SELECT EXTRACT(DOW FROM start_date) as days, COUNT(EXTRACT(DOW FROM start_date)) as days_count FROM encounter GROUP BY days ORDER BY days_count DESC LIMIT 1"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row

    def get_least_popular_day(self):
        query = """SELECT EXTRACT(DOW FROM start_date) as days, COUNT(EXTRACT(DOW FROM start_date)) as days_count FROM encounter GROUP BY days ORDER BY days_count ASC LIMIT 1"""
        self.db.cursor.execute(query)
        row = self.db.cursor.fetchone()
        return row

    def get(self, data):
        pass

    def insert(self, data):
        pass

    def update(self, data):
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

            print("Failed to insert record into patient table", error)
            sys.exit()
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
                print("Failed to insert record into procedure table", error)
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
                print("Failed to store record into observation table", error)
                return None

    def query(self, query):
        pass

    def get(self, data):
        pass

    def update(self, data):
        pass

    def delete(self, data):
        pass
