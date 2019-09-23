import psycopg2
import configparser


class DB:
    instance = None

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.host = config['DB']['host']
        self.user = config['DB']['user']
        self.port = config['DB']['port']
        self.password = config['DB']['password']
        self.database = config['DB']['database']
        self.url = "postgresql://{}:{}@{}/{}".format(self.user, self.password, self.host, self.database)
        self.connection = None
        self.cursor = None

    def init(self):
        self.connect()
        self.clear_tables()

    def connect(self):

        try:
            self.connection = psycopg2.connect(user=self.user, password=self.password, host=self.host, port=self.port,
                                               database=self.database)
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True
            self.create_tables()
            return self.connection
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)

    def create_tables(self):
        try:

            self.create_patient_table()
            self.create_encounter_table()
            self.create_observation_table()
            self.create_procedure_table()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.close()

    def clear_tables(self):
        try:
            query = """TRUNCATE TABLE patient,encounter,observation,procedure;"""
            self.cursor.execute(query)
            self.connection.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def create_patient_table(self):
        create_patient_table = '''

        CREATE SEQUENCE IF NOT EXISTS patient_id_seq
        START WITH 1
        INCREMENT BY 1  
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
        CREATE TABLE IF NOT EXISTS public.patient
                    (
                        id integer NOT NULL DEFAULT nextval('patient_id_seq'::regclass),
                        source_id text COLLATE pg_catalog."default" NOT NULL,
                        birth_date date,
                        gender text COLLATE pg_catalog."default",
                        race_code text COLLATE pg_catalog."default",
                        race_code_system text COLLATE pg_catalog."default",
                        ethnicity_code text COLLATE pg_catalog."default",
                        ethnicity_code_system text COLLATE pg_catalog."default",
                        country text COLLATE pg_catalog."default",
                        CONSTRAINT patient_pkey PRIMARY KEY (id)
                    )'''
        self.cursor.execute(create_patient_table)
        self.connection.commit()

    def create_app_table(self):
        create_app_table = '''

            CREATE SEQUENCE IF NOT EXISTS app_id_seq
            START WITH 1
            INCREMENT BY 1  
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
            CREATE TABLE IF NOT EXISTS public.app
                        (
                            id integer NOT NULL DEFAULT nextval('app_id_seq'::regclass),
                            name text COLLATE pg_catalog."default" NOT NULL,
                            started_at timestamp without time zone NOT NULL,
                            ended_at timestamp without time zone,
                            CONSTRAINT app_pkey PRIMARY KEY (id)
                        )'''
        self.cursor.execute(create_app_table)
        self.connection.commit()

    def create_encounter_table(self):

        create_encounter_table = ''' 
        
        CREATE SEQUENCE IF NOT EXISTS encounter_id_seq
        START WITH 1
        INCREMENT BY 1  
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
        
        CREATE TABLE IF NOT EXISTS public.encounter
        (
            id integer NOT NULL DEFAULT nextval('encounter_id_seq'::regclass),
            source_id text COLLATE pg_catalog."default" NOT NULL,
            patient_id integer NOT NULL,
            start_date timestamp without time zone NOT NULL,
            end_date timestamp without time zone NOT NULL,
            type_code text COLLATE pg_catalog."default",
            type_code_system text COLLATE pg_catalog."default",
            CONSTRAINT encounter_pkey PRIMARY KEY (id),
            CONSTRAINT patient FOREIGN KEY (patient_id)
            REFERENCES public.patient (id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
        )
            '''
        self.cursor.execute(create_encounter_table)
        self.connection.commit()

    def create_observation_table(self):
        create_observation_table = ''' 
        CREATE SEQUENCE IF NOT EXISTS observation_id_seq
        START WITH 1
        INCREMENT BY 1  
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
        
        CREATE TABLE IF NOT EXISTS public.observation
                (
                    id integer NOT NULL DEFAULT nextval('observation_id_seq'::regclass),
                    source_id text COLLATE pg_catalog."default" NOT NULL,
                    patient_id integer NOT NULL,
                    encounter_id integer,
                    observation_date date NOT NULL,
                    type_code text COLLATE pg_catalog."default" NOT NULL,
                    type_code_system text COLLATE pg_catalog."default" NOT NULL,
                    value numeric(15,3) NOT NULL,
                    unit_code text COLLATE pg_catalog."default",
                    unit_code_system text COLLATE pg_catalog."default",
                    CONSTRAINT observation_pkey PRIMARY KEY (id),
                    CONSTRAINT encounter FOREIGN KEY (encounter_id)
                        REFERENCES public.encounter (id) MATCH SIMPLE
                        ON UPDATE NO ACTION
                        ON DELETE NO ACTION,
                    CONSTRAINT patient FOREIGN KEY (patient_id)
                        REFERENCES public.patient (id) MATCH SIMPLE
                        ON UPDATE NO ACTION
                        ON DELETE NO ACTION
                )
                '''
        self.cursor.execute(create_observation_table)
        self.connection.commit()

    def create_procedure_table(self):

        create_procedure_table = ''' 
        CREATE SEQUENCE IF NOT EXISTS procedure_id_seq
        START WITH 1
        INCREMENT BY 1  
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
        
        CREATE TABLE IF NOT EXISTS public.procedure
        (
            id integer NOT NULL DEFAULT nextval('procedure_id_seq'::regclass),
            source_id text COLLATE pg_catalog."default" NOT NULL,
            patient_id integer NOT NULL,
            encounter_id integer,
            procedure_date date NOT NULL,
            type_code text COLLATE pg_catalog."default" NOT NULL,
            type_code_system text COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT procedure_pkey PRIMARY KEY (id),
            CONSTRAINT encounter FOREIGN KEY (encounter_id)
                REFERENCES public.encounter (id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT patient FOREIGN KEY (patient_id)
                REFERENCES public.patient (id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        )
        '''
        self.cursor.execute(create_procedure_table)
        self.connection.commit()

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
