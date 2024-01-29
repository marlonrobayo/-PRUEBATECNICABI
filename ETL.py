import pandas as pd
from sqlalchemy import create_engine
import os


print("Este programa está diseñado para importar datos con calidad a un Data Warehouse.")
print("Por favor, proporcione la información necesaria para establecer la conexión con la base de datos:")

# Solicitar datos para conexion 
usuario = input("Ingresa el nombre de usuario de la base de datos: ")
password = input("Ingresa la contraseña de la base de datos: ")
host = input("Ingresa la dirección del servidor de la base de datos (por ejemplo, localhost): ")
database_name = input("Ingresa el nombre de la base de datos: ")



fuente = None

def extract():
    global fuente  
    # Verifica si el archivo existe
    archivo_csv = os.path.join('Fuentes', 'Casos_positivos_de_COVID-19-Cund-Boy.csv')

    if os.path.exists(archivo_csv):
        fuente = pd.read_csv(archivo_csv, delimiter=';')
        print("Archivo CSV importado exitosamente.")
    else:
        print(f"El archivo CSV '{archivo_csv}' no fue encontrado.")
    return fuente



#---EXTRAER


fuente = extract()

    #-------Tranformar
  


# department
groupby_department = fuente.groupby(["id_department", "name_department"]).sum()
groupby_department = groupby_department.reset_index()[["id_department", "name_department"]]
nr_groupby_department = len(groupby_department)

# municipality
groupby_municipality = fuente.groupby(["id_municipality", "name_municipality","id_department"]).sum()
groupby_municipality = groupby_municipality.reset_index()[["id_municipality", "name_municipality","id_department"]]
  
      
# municipality
groupby_municipality = fuente.groupby(["id_municipality", "name_municipality","id_department"]).sum()
groupby_municipality = groupby_municipality.reset_index()[["id_municipality", "name_municipality","id_department"]]

# type_contagion
groupby_type_contagion = fuente.groupby(["type_contagion"]).sum()
groupby_type_contagion = groupby_type_contagion.reset_index()[["type_contagion"]]
  
# status
groupby_status = fuente.groupby(["status"]).sum()
groupby_status= groupby_status.reset_index()[["status"]]

  
# gender
groupby_gender = fuente.groupby(["gender"]).sum()
groupby_gender= groupby_gender.reset_index()[["gender"]]

# fact

groupby_fact = fuente.drop_duplicates()



# --------- Validar Datos Integridad---------------

cadena_conexion = f"mysql+pymysql://{usuario}:{password}@{host}/{database_name}"


engine = create_engine(cadena_conexion)

#engine = create_engine('mysql+pymysql://root:J3nteamo@localhost/trainingdb')  #####
#---- department 

consulta = "SELECT * FROM department"
df_department_mysql = pd.read_sql_query(consulta, con=engine)
df_fuente = groupby_department
df_destino = df_department_mysql
merged_data = pd.merge(df_fuente, df_destino, on='id_department', how='outer', indicator=True)
datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)
nr_cruzado = len(datos_coincidentes)
nr_no_cruzados = len(datos_no_coincidentes)
print(f"Ya esta en la base department {nr_cruzado} filas.")
print(f"Por Cargar department {nr_no_cruzados} filas.")
    
datos_no_coincidentes = datos_no_coincidentes[['id_department', 'name_department_x']]
datos_no_coincidentes.rename(columns={'name_department_x': 'name_department'}, inplace=True)


#---Cargar

# Envía el DataFrame a la base de datos
datos_no_coincidentes.to_sql('department', con=engine, if_exists='append', index=False)

#---- municipality 

consulta = "SELECT * FROM municipality"
df_municipality_mysql = pd.read_sql_query(consulta, con=engine)
df_fuente = groupby_municipality
df_destino = df_municipality_mysql
merged_data = pd.merge(df_fuente, df_destino, on='id_municipality', how='outer', indicator=True)
datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)
nr_cruzado = len(datos_coincidentes)
nr_no_cruzados = len(datos_no_coincidentes)
print(f"Ya esta en la basemunicipality {nr_cruzado} filas.")
print(f"Por Cargar municipality {nr_no_cruzados} filas.")

datos_no_coincidentes = datos_no_coincidentes[['id_municipality', 'name_municipality_x','id_department']]

datos_no_coincidentes.rename(columns={'name_municipality_x': 'name_municipality'}, inplace=True)
datos_no_coincidentes.rename(columns={'id_department': 'department_id'}, inplace=True)






# Envía el DataFrame a la base de datos
datos_no_coincidentes.to_sql('municipality', con=engine, if_exists='append', index=False)



#---- type_contagion 

consulta = "SELECT * FROM type_contagion"
df_consulta= pd.read_sql_query(consulta, con=engine)
df_fuente = groupby_type_contagion
df_destino = df_consulta


merged_data = pd.merge(df_fuente, df_destino.rename(columns={'contagion_name': 'type_contagion'}),
                       on='type_contagion', how='outer', indicator=True)



datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)
nr_cruzado = len(datos_coincidentes)
nr_no_cruzados = len(datos_no_coincidentes)
print(f"Ya esta en la basetype_contagion {nr_cruzado} filas.")
print(f"Por Cargar type_contagion {nr_no_cruzados} filas.")
    
datos_no_coincidentes = datos_no_coincidentes[['type_contagion']]

datos_no_coincidentes.rename(columns={'type_contagion': 'contagion_name'}, inplace=True)

# Envía el DataFrame a la base de datos
datos_no_coincidentes.to_sql('type_contagion', con=engine, if_exists='append', index=False)



#---- status 

consulta = "SELECT * FROM status"
df_consulta= pd.read_sql_query(consulta, con=engine)
df_fuente = groupby_status
df_destino = df_consulta
merged_data = pd.merge(df_fuente, df_destino.rename(columns={'statusc_name': 'status'}),
                       on='status', how='outer', indicator=True)



datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)
nr_cruzado = len(datos_coincidentes)
nr_no_cruzados = len(datos_no_coincidentes)
print(f"Ya esta en la base status { nr_cruzado} filas.")
print(f"Por Cargar status {nr_no_cruzados} filas.")
    
datos_no_coincidentes = datos_no_coincidentes[['status']]

datos_no_coincidentes.rename(columns={'status': 'statusc_name'}, inplace=True)

# Envía el DataFrame a la base de datos
datos_no_coincidentes.to_sql('status', con=engine, if_exists='append', index=False)



#---- gerden 

consulta = "SELECT * FROM gerden"
df_consulta= pd.read_sql_query(consulta, con=engine)
df_fuente = groupby_gender
df_destino = df_consulta
merged_data = pd.merge(df_fuente, df_destino.rename(columns={'gerden_name': 'gender'}),
                       on='gender', how='outer', indicator=True)

datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)
nr_cruzado = len(datos_coincidentes)
nr_no_cruzados = len(datos_no_coincidentes)
print(f"Ya esta en la base gender { nr_cruzado} filas.")
print(f"Por Cargar gender {nr_no_cruzados} filas.")
    
datos_no_coincidentes = datos_no_coincidentes[['gender']]

datos_no_coincidentes.rename(columns={'gender': 'gerden_name'}, inplace=True)
print(datos_no_coincidentes)
# Envía el DataFrame a la base de datos
datos_no_coincidentes.to_sql('gerden', con=engine, if_exists='append', index=False)



#---- Fact 


consulta = "SELECT * FROM type_contagion"
df_r= pd.read_sql_query(consulta, con=engine)
df_destino = df_r
df_fuente= groupby_fact



merged_df = pd.merge(df_fuente, df_destino, how='left', left_on='type_contagion', right_on='contagion_name')



consulta = "SELECT * FROM status"
df_r= pd.read_sql_query(consulta, con=engine)

df_destino = df_r


merged_df = pd.merge( merged_df,df_r, how='left', left_on='status', right_on='statusc_name')


merged_df['gender'] = merged_df['gender'].apply(lambda x: 1 if x == 'M' else (2 if x == 'F' else None))


merged_df = merged_df[['id_case', 'id_municipality', 'age', 'idtype_contagion', 'idstatus',
       'gender', 'date_symptom', 'date_death', 'date_diagnosis',
       'date_recovery']]


merged_df.rename(columns={'idtype_contagion': 'id_type_contagion', 'idstatus': 'id_status', 'gender': 'id_gerden','date_diagnosis':'date_diacnosis'}, inplace=True)


consulta = "SELECT * FROM cases_fact"
df_destino= pd.read_sql_query(consulta, con=engine)

merged_data = pd.merge(merged_df, df_destino, on='id_case', how='outer', indicator=True)

datos_coincidentes = merged_data[merged_data['_merge'] == 'both'].drop('_merge', axis=1)
datos_no_coincidentes = merged_data[merged_data['_merge'] == 'left_only'].drop('_merge', axis=1)

datos_no_coincidentes = datos_no_coincidentes[['id_case', 'id_municipality_x', 'age_x', 'id_type_contagion_x',
       'id_status_x', 'id_gerden_x', 'date_symptom_x', 'date_death_x',
       'date_diacnosis_x', 'date_recovery_x']]

datos_no_coincidentes.rename(columns={'id_municipality_x':'id_municipality', 'age_x':'age', 'id_type_contagion_x':'id_type_contagion',
       'id_status_x':'id_status', 'id_gerden_x':'id_gerden', 'date_symptom_x':'date_symptom', 'date_death_x':'date_death',
       'date_diacnosis_x':'date_diacnosis', 'date_recovery_x':'date_recovery'}, inplace=True)

datos_no_coincidentes['date_symptom'] = pd.to_datetime(datos_no_coincidentes['date_symptom'], format='%d/%m/%Y')
datos_no_coincidentes['date_death'] = pd.to_datetime(datos_no_coincidentes['date_death'], format='%d/%m/%Y')
datos_no_coincidentes['date_diacnosis'] = pd.to_datetime(datos_no_coincidentes['date_diacnosis'], format='%d/%m/%Y')
datos_no_coincidentes['date_recovery'] = pd.to_datetime(datos_no_coincidentes['date_recovery'], format='%d/%m/%Y')

cantidad_columnas_alternativa = len(datos_no_coincidentes.columns)

datos_no_coincidentes.to_sql('cases_fact', con=engine, if_exists='append', index=False)



