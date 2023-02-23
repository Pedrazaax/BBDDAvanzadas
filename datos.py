import csv
import sqlite3

conn = sqlite3.connect('salarios.db')
datos = []

def cargarDatos():
    csvs =['BBDDAvanzadas/microdatos/EES_2002.csv','BBDDAvanzadas/microdatos/EES_2006.csv','BBDDAvanzadas/microdatos/EES_2010.csv','BBDDAvanzadas/microdatos/EES_2014.csv','BBDDAvanzadas/microdatos/EES_2018.csv']  #Para cargar todos los csvs a la vez
    contCuatrienal= 1 #Sirve para saber en que cuatrianual estamos sabiendo que empezamos en 1995
    # la primera fila la toma para el nombre de los campos y a partir de ahi añade los datos
    for archivo in csvs:   #archivo representa al csv que se va a cargar
        with open(archivo, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                row['CUATRIENAL'] = contCuatrienal
                if(archivo == 'BBDDAvanzadas/microdatos/EES_2018.csv'):
                    row['ORDENCCC'] = row.pop('IDENCCC')
                datos.append(row)
        contCuatrienal = contCuatrienal +1


def crearTabla():
    
    cursor = conn.cursor()
    cursor.execute("drop table if exists datosMercadoLaboral")
    cursor.execute("CREATE TABLE datosMercadoLaboral (ORDENCCC INTEGER,CONTROL INTEGER, RESPONSA INTEGER, NUTS1 INTEGER, EXTRAORM INTEGER, SALBASE INTEGER, IRPFMES INTEGER, ANOANTI INTEGER, MERCADO INTEGER, TIPOJOR INTEGER, FACTOTAL INTEGER, VAL INTEGER, CNO1 TEXT, MESANTI INTEGER, COMSALTT INTEGER, COTIZA INTEGER, VAN INTEGER, ESTU INTEGER, ESTRATO2 INTEGER, PHEXTRA INTEGER, COMSAL INTEGER, ORDENTRA INTEGER, SEXO INTEGER, PUENTES INTEGER, HEXTRA INTEGER, ANOS2 INTEGER, JAP INTEGER, CNACE TEXT, TIPOPAIS INTEGER, CUATRIENAL INTEGER)")
    conn.commit()


def insercionDatos():
    #query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1, CNACE, ESTRATO2, CONTROL, MERCADO, SEXO, TIPOPAIS, CNO1, RESPONSA, ESTU, MESANTI, ANOANTI, TIPOJOR, VAL, VAN, PUENTES, JAP, JSP1, JSP2, HEXTRA, SALBASE, EXTRAORM, PHEXTRA, COMSAL, COMSALTT, IRPFMES, COTIZA, DIAS, SALBRUTO, PEXTRAAF, PEXTRAAV, VESP, AFECANO, MESCOMA, DIACOMA, ANOS2, FACTOTAL, CUATRIENAL) VALUES (:ORDENCCC, :ORDENTRA, :NUTS1, :CNACE, :ESTRATO2, :CONTROL, :MERCADO, :SEXO, :TIPOPAIS, :CNO1, :RESPONSA, :ESTU, :MESANTI, :ANOANTI, :TIPOJOR, :VAL, :VAN, :PUENTES, :JAP, :JSP1,:JSP2, :HEXTRA, :SALBASE, :EXTRAORM, :PHEXTRA, :COMSAL, :COMSALTT, :IRPFMES, :COTIZA, :DIAS, :SALBRUTO, :PEXTRAAF, :PEXTRAAV, :VESP, :AFECANO, :MESCOMA, :DIACOMA, :ANOS2, :FACTOTAL, :CUATRIENAL)"
    #query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1,CNACE,ESTRATO2,CONTROL,MERCADO,CONVENIO,SEXO,TIPOPAIS,CNO1,RESPONSA,ESTU,MESANTI,ANOANTI,TIPOJOR,TC,VAL,VAN,PUENTES,JAP,JSP,HEXTRA,SALBASE,EXTRAORM,PHEXTRA,COMSAL,COMSALTT,COMSALV,IRPFMES,COTIZA,AFECMES,DIAS,SALBRUTO,PEXTRAAF,PEXTRAAV,VESP,AFECANO,MESCOMA,DIACOMA,ANOS2,FACTOTAL,CUATRIENAL) VALUES (?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?,?,?)"
    query = "INSERT INTO datosMercadoLaboral (ORDENCCC ,CONTROL, RESPONSA, NUTS1, EXTRAORM, SALBASE, IRPFMES, ANOANTI, MERCADO, TIPOJOR, FACTOTAL, VAL, CNO1, MESANTI, COMSALTT, COTIZA, VAN, ESTU, ESTRATO2, PHEXTRA, COMSAL, ORDENTRA, SEXO, PUENTES, HEXTRA, ANOS2, JAP, CNACE, TIPOPAIS, CUATRIENAL) VALUES (:ORDENCCC, :CONTROL, :RESPONSA, :NUTS1, :EXTRAORM, :SALBASE, :IRPFMES, :ANOANTI, :MERCADO, :TIPOJOR, :FACTOTAL, :VAL, :CNO1, :MESANTI, :COMSALTT, :COTIZA, :VAN, :ESTU, :ESTRATO2, :PHEXTRA, :COMSAL, :ORDENTRA, :SEXO, :PUENTES, :HEXTRA, :ANOS2, :JAP, :CNACE, :TIPOPAIS, :CUATRIENAL)"
    conn.executemany(query, datos)
    print("Datos insertados con exito")
    #cursor = conn.cursor()


    #cursor.execute("SELECT * FROM datosMercadoLaboral")
    #resultados = cursor.fetchall()

    #for fila in resultados:
    #    print(fila)


if __name__ == "__main__":
   cargarDatos()
   crearTabla()
   insercionDatos()
   #print(datos[0])
   #print(datos[100000]["cuatrienal"])