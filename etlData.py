import pandas as pd 
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

#Conectamos al motor de base de datos para crear la base de datos
engine = create_engine('mysql+mysqlconnector://Yeison:1123@localhost')

# Creamos la base de datos jobsML si no existe
with engine.connect() as conn:
    conn.execute(text('CREATE DATABASE IF NOT EXISTS jobsML'))


#Conectamos a la base de datos jobsML
engine = create_engine('mysql+mysqlconnector://Yeison:1123@localhost/jobsML')

#Definimos la Base y la session
Base = declarative_base()
Session = sessionmaker(bind=engine)

#Extraemos los datos del archivo CSV
data = pd.read_csv('1000_ml_jobs_us.csv')

#Limpiamos los datos
data = data.dropna()
data = data.drop_duplicates()

class Posting(Base):
    __tablename__ = 'postings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    company_name = Column(String(255), nullable=False)
    company_locality = Column(String(255), nullable=False)
    level = Column(String(50), nullable=False)
    job_title = Column(String(200), nullable=False)

if __name__ == '__main__':
    # Creamos la tabla si no existe
    Base.metadata.drop_all(engine)  # Eliminar la tabla si existe (opcional)
    Base.metadata.create_all(engine)

    # Creamos una sesion para insertar los datos
    session = Session()
    
    data['job_posted_date'] = pd.to_datetime(data['job_posted_date'])
    # Insertamos los datos en la base de datos
    
    """for  _, row in data.iterrows():
        print(row)
        session.add(Posting(
                            date = row['job_posted_date'].to_pydatetime(), 
                            company_name = row['company_name'],
                            company_locality = row['company_address_locality'],
                            level = row['seniority_level'],
                            job_title = row['job_title']))"""
    
    
    for i in range(len(data)):
        session.add(Posting(date = data['job_posted_date'].iloc[i].to_pydatetime(), 
                            company_name = data['company_name'].iloc[i],
                            company_locality = data['company_address_locality'].iloc[i],
                            level = data['seniority_level'].iloc[i],
                            job_title = data['job_title'].iloc[i]))
        

        
    session.commit()
    session.close()




