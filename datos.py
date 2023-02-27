#! /usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import sqlite3
import pandas as pd

conn = sqlite3.connect('salarios.db')
conn.execute("PRAGMA foreign_keys = 1")
datos = []


def cargarDatos():
    csvs = ['./microdatos/EES_2010.csv', './microdatos/EES_2014.csv',
            './microdatos/EES_2018.csv']  # Para cargar todos los csvs a la vez
    # Sirve para saber en que cuatrianual estamos sabiendo que empezamos en 2010
    contCuatrienal = 1
    # la primera fila la toma para el nombre de los campos y a partir de ahi añade los datos
    for archivo in csvs:  # archivo representa al csv que se va a cargar
        with open(archivo, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                transformacion(contCuatrienal, archivo, row)
                datos.append(row)
        contCuatrienal = contCuatrienal + 1


def transformacion(contCuatrienal, archivo, row):
    row['CUATRIENAL'] = contCuatrienal
    if (archivo == './microdatos/EES_2018.csv'):
        # Esto lo hacemos porque a partir del 2018 ordenccc no existe
        row['ORDENCCC'] = row.pop('IDENCCC')

    if (archivo == './microdatos/EES_2010.csv'):
        estudio = int(row['ESTU'])
        if (estudio > 5):
            estudio = estudio - 1
            row['ESTU'] = estudio


def crearTablaDimensiones():

    cursor = conn.cursor()
    cursor.execute("drop table if exists datosMercadoLaboral")
    cursor.execute(
        "CREATE TABLE control (id INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE zona (idZona INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE mercado (idMercado INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE jornada (idJornada INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE puesto (idPuesto TEXT PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE estudios (idEstudio INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE tamanioEmpresa (idTamanioEmpresa INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE sexo (idSexo INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE edad (idEdad INTEGER PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE sector (idSector TEXT PRIMARY KEY, descripcion TEXT)")
    cursor.execute(
        "CREATE TABLE pais (idPais INTEGER PRIMARY KEY, descripcion TEXT)")

    conn.commit()


def insertarDatosDimensiones():
    cursor = conn.cursor()
    cursor.execute("INSERT INTO control (id, descripcion) VALUES (?, ?), (?, ?)",
                   (1, "PUBLICO", 2, "PRIVADO"))
    cursor.execute("INSERT INTO zona (idZona, descripcion) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)",
                   (1, "NOROESTE", 2, "NORESTE", 3, "COMUNIDAD DE MADRID", 4, "CENTRO", 5, "ESTE", 6, "SUR", 7, "CANARIAS"))
    cursor.execute("INSERT INTO mercado (idMercado, descripcion) VALUES (?, ?), (?, ?), (?, ?), (?, ?)",
                   (1, "LOCAL O REGIONAL", 2, "NACIONAL", 3, "UNIÓN EUROPEA", 4, "MUNDIAL"))
    cursor.execute("INSERT INTO jornada (idJornada, descripcion) VALUES (?, ?), (?, ?), (?, ?)",
                   (1, "TIEMPO COMPLETO", 2, "TIEMPO PARCIAL", 6, "TIEMPO PARCIAL"))
    cursor.execute("INSERT INTO puesto (idPuesto, descripcion) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)",
                   ("A0", "DIRECTORES Y GERENTES",
                    "B0", "TÉCNICOS Y PROFESIONALES CIENTÍFICOS E INTELECTUALES DE LA SALUD Y LA ENSEÑANZA",
                    "C0", "OTROS TÉCNICOS Y PROFESIONALES CIENTÍFICOS E INTELECTUALES",
                    "D0", "TÉCNICOS; PROFESIONALES DE APOYO",
                    "E0", "EMPLEADOS DE OFICINA QUE NO ATIENDEN AL PÚBLICO",
                    "F0", "EMPLEADOS DE OFICINA QUE ATIENDEN AL PÚBLICO",
                    "G0", "TRABAJADORES DE LOS SERVICIOS DE RESTAURACION Y COMERCIO",
                    "H0", "TRABAJADORES DE LOS SERVICIOS DE SALUD Y EL CUIDADO DE PERSONAS",
                    "I0", "TRABAJADORES DE LOS SERVICOS DE PROTECCION Y SEGURIDAD",
                    "J0", "TRABAJADORES CUALIFICADOS EN EL SECTOR AGRÍCOLA, GANADERO, FORESTAL Y PESQUERO",
                    "K0", "TRABAJADORES CUALIFICADOS DE LA CONSTRUCCIÓN, EXCEPTO LOS OPERADORES DE MÁQUINAS",
                    "L0", "TRABAJADORES CUALIFICADOS DE LAS INDUSTRIAS MANUFACTURERAS, EXCEPTO OPERADORES DE INSTALACIONES Y MÁQUINAS",
                    "M0", "OPERADORES DE INSTALACIONES Y MAQUINARIA FIJAS, Y MONTADORES",
                    "N0", "CONDUCTORES Y OPERADORES DE MAQUINARIA MOVIL",
                    "O0", "TRABAJADORES NO CUALIFICADOS EN SERVICIOS",
                    "P0", "PEONES DE LA AGRICULTURA, PESCA, CONSTRUCCIÓN, INDUSTRIAS MANUFACTURERAS Y TRANSPORTES",
                    "Q0", "OCUPACIONES MILITARES"))

    cursor.execute("INSERT INTO estudios (idEstudio, descripcion) VALUES (1, 'Menos que primaria'), \
              (2, 'Educación primaria'), \
              (3, 'Primera etapa de educación secundaria'), \
              (4, 'Segunda etapa de eduación secundaria'), \
              (5, 'Enseñanzas de formación profesional de grado superior y similares'), \
              (6, 'Diplomados universitarios y similares'), \
              (7, 'Licenciados y similares, y doctores universitarios')")

    cursor.execute("INSERT INTO tamanioEmpresa (idTamanioEmpresa, descripcion) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)", (0,
                   'INCLUYE TODOS LOS ESTRATOS', 1, 'DE 1  A 49 TRABAJADORES', 2, 'DE 50 A 199 TRABAJADORES', 3, '200 Y MÁS TRABAJADORES', 4, 'INCLUYE ESTRATO 2 Y 3'))
    cursor.execute(
        "INSERT INTO sexo (idSexo, descripcion) VALUES (?, ?), (?, ?)", (1, 'HOMBRE', 6, 'MUJER'))
    cursor.execute("INSERT INTO edad (idEdad, descripcion) VALUES (?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)", ('01',
                   'MENOS 19 AÑOS', '02', 'DE 20 A 29', '03', 'DE 30 A 39', '04', 'DE 40 A 49', '05', 'DE 50 A 59', '06', 'MÁS DE 59'))
    cursor.execute("INSERT INTO sector (idSector, descripcion) VALUES ('B0', 'Industrias extractivas: extracción de antracita, hulla y lignito, extracción de crudo de petróleo y gas natural, extracción de minerales metálicos, otras industrias extractivas, actividades de apoyo a las industrias extractivas'), ('C1', 'Industria manufacturera: industria de la alimentación, fabricación de bebidas, industria del tabaco, industria textil, confección de prendas de vestir, industria del cuero y del calzado'), ('C2', 'Industria manufacturera: industria de la madera y del corcho, excepto muebles; cestería y espartería, industria del papel'), ('C3', 'Industria manufacturera: artes gráficas y reproducción de soportes grabados'), ('C4', 'Industria manufacturera: coquerías y refino de petróleo, industria química, fabricación de productos farmacéuticos, fabricación de productos de caucho y plásticos'), ('C5', 'Industria manufacturera: fabricación de otros productos minerales no metálicos'), ('C6', 'Industria manufacturera: metalurgia; fabricación de productos de hierro, acero y ferroaleaciones, fabricación de productos metálicos, excepto maquinaria y equipo'), ('C7', 'Industria manufacturera: fabricación de productos informáticos, electrónicos y ópticos, fabricación de material y equipo eléctrico, fabricación de maquinaria y equipo no contemplado en otras partes'), ('C8', 'Industria manufacturera: fabricación de vehículos de motor, remolques y semirremolques, fabricación de otro material de transporte, fabricación de muebles, otras industrias manufactureras, reparación e instalación de maquinaria y equipo'), ('D0', 'Suministro de energía eléctrica, gas, vapor y aire acondicionado'), ('E0', 'Suministro de agua, actividades de saneamiento, gestión de residuos y descontaminación: captación, depuración y distribución de agua, recogida y tratamiento de aguas residuales, recogida, tratamiento y eliminación de residuos; valorización, actividades de descontaminación y otros servicios de gestión de residuos'), ('F0', 'Construcción: construcción de edificios, ingeniería civil, actividades de construcción especializada'), ('G1', 'Comercio al por mayor y al por menor; reparación de vehículos de motor y motocicletas: venta y reparación de vehículos de motor y motocicletas, comercio al por mayor e intermediarios del comercio, excepto de vehículos de motor y motocicletas'), ('G2', 'Comercio al por mayor y al por menor; reparación de vehículos de motor y motocicletas: comercio al por menor, excepto de vehículos de motor y motocicletas'), ('H1', 'Transporte y almacen terrestre'),('H2', 'Transporte y almacen'),('I0', 'Hostelería: servicios de alojamiento, servicios de comidas y bebidas'),('J0', 'Información y comunicaciones: edición, actividades cinematográficas, de vídeo y de programas de televisión, grabación de sonido y edición musical, actividades de programación y emisión de radio y televisión, telecomunicaciones, programación, consultoría y otras actividades relacionadas con la informática, servicios de información'),('K0', 'Actividades financieras y de seguros: servicios financieros, excepto seguros y fondos de pensiones, seguros, reaseguros y fondos de pensiones, excepto seguridad social obligatoria, actividades auxiliares a los servicios financieros y a los seguros'),('L0', 'Actividades inmobiliarias'),('M0', 'Actividades profesionales, científicas y técnicas: actividades jurídicas y de contabilidad, actividades de las sedes centrales; actividades de consultoría de gestión empresarial, servicios técnicos de arquitectura e ingeniería; ensayos y análisis técnicos, investigación y desarrollo, publicidad y estudios de mercado, otras actividades profesionales, científicas y técnicas, actividades veterinarias'),('N0', 'Actividades administrativas y servicios auxliares: actividades de alquiler, actividades relacionadas con el empleo, actividades de agencias de viajes, operadores turísticos, servicios de reservas y actividades relacionadas con los mismos, actividades de seguridad e investigación, servicios a edificios y actividades de jardinería, actividades administrativas de oficina y otras actividades auxiliares a las empresas'),('O0', 'Administración Pública y defensa; Seguridad Social obligatoria'),('P0', 'Educación'),('Q0', 'Actividades sanitarias y de servicios sociales: actividades sanitarias, asistencia en establecimientos residenciales, actividades de servicios sociales sin alojamiento'),('R0', 'Actividades artísticas, recreativas y de entrenimiento: actividades de creación, artísticas y espectáculos, actividades de bibliotecas, archivos, museos y otras actividades culturales, actividades de juegos de azar y apuestas, actividades deportivas, recreativas y de entretenimiento'),('S0', 'Otros servicios: actividades asociativas, reparación de ordenadores, efectos personales y artículos de uso doméstico, otros servicios personales')")
    cursor.execute("INSERT INTO pais (idPais, Descripcion) VALUES (?, ?), (?, ?)",
                   (1, "ESPAÑA", 2, "RESTO MUNDO"))

    conn.commit()


def insercionDatos():
    query = "INSERT INTO datosMercadoLaboral (ORDENCCC ,idControl , idZona , EXTRAORM , SALBASE , IRPFMES , ANOANTI , idMercado , idJornada , FACTOTAL , VAL , idPuesto , MESANTI , COMSALTT , COTIZA , VAN , idEstudio , idTamanioEmpresa , PHEXTRA , COMSAL , ORDENTRA , idSexo , PUENTES , HEXTRA , idEdad , JAP , idSector , idPais , idCuatrienal) VALUES (:ORDENCCC, :CONTROL, :NUTS1, :EXTRAORM, :SALBASE, :IRPFMES, :ANOANTI, :MERCADO, :TIPOJOR, :FACTOTAL, :VAL, :CNO1, :MESANTI, :COMSALTT, :COTIZA, :VAN, :ESTU, :ESTRATO2, :PHEXTRA, :COMSAL, :ORDENTRA, :SEXO, :PUENTES, :HEXTRA, :ANOS2, :JAP, :CNACE, :TIPOPAIS, :CUATRIENAL)"
    try:
        conn.executemany(query, datos)
        print("Datos insertados con éxito")
    except sqlite3.DatabaseError as e:
        print("Error en la inserción de datos:", e)

    conn.commit()


def crearTablaHechos():
    cursor = conn.cursor()
    #cursor.execute("CREATE TABLE datosMercadoLaboral (ORDENCCC INTEGER ,idControl INTEGER, idZona INTEGER, EXTRAORM INTEGER, SALBASE INTEGER, IRPFMES INTEGER, ANOANTI INTEGER, idMercado INTEGER, idJornada INTEGER, FACTOTAL INTEGER, VAL INTEGER, idPuesto TEXT, MESANTI INTEGER, COMSALTT INTEGER, COTIZA INTEGER, VAN INTEGER, idEstudio INTEGER, idTamanioEmpresa INTEGER, PHEXTRA INTEGER, COMSAL INTEGER, ORDENTRA INTEGER, idSexo INTEGER, PUENTES INTEGER, HEXTRA INTEGER, idEdad INTEGER, JAP INTEGER, idSector TEXT, idPais INTEGER, idCuatrienal INTEGER, FOREIGN KEY(idControl) REFERENCES control(id), FOREIGN KEY(idZona) REFERENCES zona(idZona), FOREIGN KEY(idMercado) REFERENCES mercado(idMercado), FOREIGN KEY(idJornada) REFERENCES jornada(idJornada), FOREIGN KEY(idPuesto) REFERENCES puesto(idPuesto), FOREIGN KEY(idEstudio) REFERENCES estudios(idEstudio), FOREIGN KEY(idTamanioEmpresa) REFERENCES tamanioEmpresa(idTamanioEmpresa), FOREIGN KEY(idSexo) REFERENCES sexo(idSexo), FOREIGN KEY(idEdad) REFERENCES edad(idEdad), FOREIGN KEY(idSector) REFERENCES sector(idSector), FOREIGN KEY(idPais) REFERENCES pais(idPais))")
    cursor.execute("CREATE TABLE datosMercadoLaboral (\
                  ORDENCCC INTEGER, \
                  idControl INTEGER,\
                  idZona INTEGER,\
                  EXTRAORM INTEGER,\
                  SALBASE INTEGER,\
                  IRPFMES INTEGER,\
                  ANOANTI INTEGER,\
                  idMercado INTEGER,\
                  idJornada INTEGER,\
                  FACTOTAL INTEGER,\
                  VAL INTEGER,\
                  idPuesto TEXT,\
                  MESANTI INTEGER,\
                  COMSALTT INTEGER,\
                  COTIZA INTEGER,\
                  VAN INTEGER,\
                  idEstudio INTEGER,\
                  idTamanioEmpresa INTEGER,\
                  PHEXTRA INTEGER,\
                  COMSAL INTEGER,\
                  ORDENTRA INTEGER,\
                  idSexo INTEGER,\
                  PUENTES INTEGER,\
                  HEXTRA INTEGER,\
                  idEdad INTEGER,\
                  JAP INTEGER,\
                  idSector TEXT,\
                  idPais INTEGER,\
                  idCuatrienal INTEGER,\
                  FOREIGN KEY(idControl) REFERENCES control(id),\
                  FOREIGN KEY(idZona) REFERENCES zona(idZona),\
                  FOREIGN KEY(idMercado) REFERENCES mercado(idMercado),\
                  FOREIGN KEY(idTamanioEmpresa) REFERENCES tamanioEmpresa(idTamanioEmpresa),\
                  FOREIGN KEY(idSexo) REFERENCES sexo(idSexo),\
                  FOREIGN KEY(idEdad) REFERENCES edad(idEdad),\
                  FOREIGN KEY(idJornada) REFERENCES jornada(idJornada),\
                  FOREIGN KEY(idPuesto) REFERENCES puesto(idPuesto),\
                  FOREIGN KEY(idEstudio) REFERENCES estudios(idEstudio),\
                  FOREIGN KEY(idSector) REFERENCES sector(idSector),\
                  FOREIGN KEY(idPais) REFERENCES pais(idPais)\
                  )")
    # Las claves ajenas que fallan
    # FOREIGN KEY(idJornada) REFERENCES jornada(idJornada),\  FOREIGN KEY(idPuesto) REFERENCES puesto(idPuesto),\   FOREIGN KEY(idEstudio) REFERENCES estudios(idEstudio),\ FOREIGN KEY(idSector) REFERENCES sector(idSector),\
    conn.commit()

def crearBBDD():
    cargarDatos()
    crearTablaDimensiones()
    insertarDatosDimensiones()
    crearTablaHechos()
    insercionDatos()

def consultas():
    cursor = conn.cursor()
    # Diferencia salarial entre géneros, según zona y sector
    cursor.execute("SELECT zona.descripcion AS Zona, sector.descripcion AS Sector, AVG(CASE WHEN idSexo = 1 THEN SALBASE END) - AVG(CASE WHEN idSexo = 6 THEN SALBASE END) AS DiferenciaSalarial FROM datosMercadoLaboral JOIN zona ON datosMercadoLaboral.idZona = zona.idZona JOIN sector ON datosMercadoLaboral.idSector = sector.idSector WHERE idSexo IN (1, 6) GROUP BY zona.idZona, sector.idSector ORDER BY Sector")
    # no funciona cursor.execute("SELECT pais.descripcion AS Nacionalidad,sexo.descripcion AS Genero, AVG(CASE WHEN pais.idPais = 1 AND sexo.idSexo = 1 THEN SALBASE END) AS SalarioHombresEspañoles, AVG(CASE WHEN pais.idPais = 1 AND sexo.idSexo = 6 THEN SALBASE END) AS SalarioMujeresEspañolas, AVG(CASE WHEN pais.idPais = 2 AND sexo.idSexo = 1 THEN SALBASE END) AS SalarioHombresExtranjeros, AVG(CASE WHEN pais.idPais = 2 AND sexo.idSexo = 6 THEN SALBASE END) AS SalarioMujeresExtranjeras FROM datosMercadoLaboral JOIN pais ON datosMercadoLaboral.idPais = pais.idPais JOIN sexo ON datosMercadoLaboral.idSexo = sexo.idSexo WHERE pais.idPais IN (1, 2) AND sexo.idSexo IN (1, 6) GROUP BY pais.descripcion, sexo.descripcion")
    resultados = cursor.fetchall()
    for row in resultados:
        print(row)


def consultaSalTit():
    # Relación entre salario según la titulación, en la zona donde se ejerce, sexo y sector.
    cursor = conn.cursor()

    consulta = '''
        SELECT estudios.descripcion AS titulacion, zona.descripcion AS zona, sexo.descripcion AS sexo, sector.descripcion AS sector, AVG(datosMercadoLaboral.SALBASE) AS salario_medio
        FROM datosMercadoLaboral
        JOIN estudios ON datosMercadoLaboral.idEstudio = estudios.idEstudio
        JOIN zona ON datosMercadoLaboral.idZona = zona.idZona
        JOIN sexo ON datosMercadoLaboral.idSexo = sexo.idSexo
        JOIN sector ON datosMercadoLaboral.idSector = sector.idSector
        GROUP BY estudios.descripcion, zona.descripcion, sexo.descripcion, sector.descripcion
    '''

    dataframe = pd.read_sql_query(consulta, conn)

    cursor.execute(consulta)

    conn.close()
    dataframe.to_excel('./consultaSalTit.xlsx', index=False)

def consultaDifSal():
    conn = sqlite3.connect('salarios.db')
    # Diferencia salarial entre géneros, según zona y sector
    cursor = conn.cursor()

    consulta = """
        SELECT zona.descripcion AS Zona, sector.descripcion AS Sector, AVG(CASE WHEN idSexo = 1 THEN SALBASE END) - AVG(CASE WHEN idSexo = 6 THEN SALBASE END) AS DiferenciaSalarial 
        FROM datosMercadoLaboral 
        JOIN zona ON datosMercadoLaboral.idZona = zona.idZona 
        JOIN sector ON datosMercadoLaboral.idSector = sector.idSector 
        WHERE idSexo IN (1, 6) GROUP BY sector.idSector,zona.idZona ORDER BY Sector
    """

    dataframe = pd.read_sql_query(consulta, conn)

    cursor.execute(consulta)

    conn.close()
    dataframe.to_excel('./consultaDifSal.xlsx', index=False)

def consultaSalAnt():
    conn = sqlite3.connect('salarios.db')
    # Porcentaje de incremento de sueldo medio de 2 3 5 y 10 años de antiguedad respecto al primer año
    cursor = conn.cursor()

    consulta = """
        SELECT ANOANTI, AVG(SALBASE) AS SalarioPromedio, 100 * (AVG(SALBASE) / (SELECT AVG(SALBASE) 
        FROM datosMercadoLaboral WHERE ANOANTI = 1)) - 100 AS DiferenciaPorcentual 
        FROM datosMercadoLaboral WHERE ANOANTI IN (1, 2, 3, 5, 10) GROUP BY ANOANTI ORDER BY ANOANTI ASC
    """
    
    dataframe = pd.read_sql_query(consulta, conn)

    cursor.execute(consulta)

    conn.close()
    dataframe.to_excel('./consultaSalAnt.xlsx', index=False)

def consultaSalFuturo():
    conn = sqlite3.connect('salarios.db')
    # ¿Cuánto cobrara según sector en los próximos 4 años?
    cursor = conn.cursor()

    consulta = '''
        SELECT sector.descripcion, datosMercadoLaboral.idCuatrienal, AVG(datosMercadoLaboral.SALBASE) AS salario_medio
        FROM datosMercadoLaboral
        JOIN sector ON datosMercadoLaboral.idSector = sector.idSector
        GROUP BY sector.descripcion, datosMercadoLaboral.idCuatrienal
    '''

    dataframe = pd.read_sql_query(consulta, conn)

    cursor.execute(consulta)

    conn.close()
    dataframe.to_excel('./consultaSalFuturo.xlsx', index=False)

def consultaJornada():
    conn = sqlite3.connect('salarios.db')
    # Titulación de las personas que trabajan media jornada, sexo y sector.

    cursor = conn.cursor()

    consulta = '''
        SELECT estudios.descripcion, sexo.descripcion, sector.descripcion, COUNT(*) AS count
        FROM datosMercadoLaboral
        JOIN estudios ON datosMercadoLaboral.idEstudio = estudios.idEstudio
        JOIN sexo ON datosMercadoLaboral.idSexo = sexo.idSexo
        JOIN sector ON datosMercadoLaboral.idSector = sector.idSector
        WHERE datosMercadoLaboral.idJornada = 2
        GROUP BY estudios.descripcion, sexo.descripcion, sector.descripcion
    '''

    dataframe = pd.read_sql_query(consulta, conn)

    cursor.execute(consulta)

    conn.close()
    dataframe.to_excel('./consultaJornada.xlsx', index=False)


if __name__ == "__main__":
    
    crearBBDD()
    consultaDifSal()
    consultaSalAnt()
    consultaSalTit()
    consultaSalFuturo()
    consultaJornada()

    conn.close()
