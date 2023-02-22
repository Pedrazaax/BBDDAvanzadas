import csv
import sqlite3

conn = sqlite3.connect('salarios.db')
datos = []

def cargarDatos():
    csvs =['BBDDAvanzadas/microdatos/EES_2018.csv']  #Para cargar todos los csvs a la vez
    contCuatrienal= 1 #Sirve para saber en que cuatrianual estamos sabiendo que empezamos en 1995
    # la primera fila la toma para el nombre de los campos y a partir de ahi añade los datos
    for archivo in csvs:   #archivo representa al csv que se va a cargar
        with open(archivo, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                row['CUATRIENAL'] = contCuatrienal
                datos.append(row)
                
        contCuatrienal = contCuatrienal +1

def crearTabla():
    
    cursor = conn.cursor()
    cursor.execute("drop table if exists datosMercadoLaboral")
    cursor.execute("CREATE TABLE datosMercadoLaboral (IDENCCC INTEGER, ORDENTRA INTEGER, NUTS1 INTEGER, CNACE TEXT, ESTRATO2 INTEGER, CONTROL INTEGER, MERCADO INTEGER, REGULACION INTEGER, SEXO INTEGER, TIPOPAIS INTEGER, CNO1 TEXT, RESPONSA INTEGER, ESTU INTEGER, ANOANTI INTEGER, MESANTI INTEGER, TIPOJOR INTEGER, TIPOCON INTEGER, FIJODISM INTEGER, FIJODISD INTEGER, VAL INTEGER, VAN INTEGER, PUENTES INTEGER, JAP INTEGER, JSP1 INTEGER, JSP2 INTEGER, HEXTRA INTEGER, DRELABM INTEGER, SIESPM1 INTEGER, DSIESPM1 INTEGER, SIESPM2 INTEGER, DSIESPM2 INTEGER, SALBASE INTEGER, EXTRAORM INTEGER, PHEXTRA INTEGER, COMSAL INTEGER, COMSALTT INTEGER, IRPFMES INTEGER, COTIZA INTEGER, BASE INTEGER, DRELABAM INTEGER, DRELABAD INTEGER, SIESPA1 INTEGER, DSIESPA1 INTEGER, SIESPA2 INTEGER, DSIESPA2 INTEGER, SIESPA3 INTEGER, DSIESPA3 INTEGER, SIESPA4 INTEGER, DSIESPA4 INTEGER, RETRINOIN INTEGER, RETRIIN INTEGER, GEXTRA INTEGER, VESPNOIN INTEGER, VESPIN INTEGER, ANOS2 INTEGER, FACTOTAL INTEGER, CUATRIENAL INTEGER)")
    conn.commit()

def insercionDatos():
    #query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1, CNACE, ESTRATO2, CONTROL, MERCADO, SEXO, TIPOPAIS, CNO1, RESPONSA, ESTU, MESANTI, ANOANTI, TIPOJOR, VAL, VAN, PUENTES, JAP, JSP1, JSP2, HEXTRA, SALBASE, EXTRAORM, PHEXTRA, COMSAL, COMSALTT, IRPFMES, COTIZA, DIAS, SALBRUTO, PEXTRAAF, PEXTRAAV, VESP, AFECANO, MESCOMA, DIACOMA, ANOS2, FACTOTAL, CUATRIENAL) VALUES (:ORDENCCC, :ORDENTRA, :NUTS1, :CNACE, :ESTRATO2, :CONTROL, :MERCADO, :SEXO, :TIPOPAIS, :CNO1, :RESPONSA, :ESTU, :MESANTI, :ANOANTI, :TIPOJOR, :VAL, :VAN, :PUENTES, :JAP, :JSP1,:JSP2, :HEXTRA, :SALBASE, :EXTRAORM, :PHEXTRA, :COMSAL, :COMSALTT, :IRPFMES, :COTIZA, :DIAS, :SALBRUTO, :PEXTRAAF, :PEXTRAAV, :VESP, :AFECANO, :MESCOMA, :DIACOMA, :ANOS2, :FACTOTAL, :CUATRIENAL)"
    #query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1,CNACE,ESTRATO2,CONTROL,MERCADO,CONVENIO,SEXO,TIPOPAIS,CNO1,RESPONSA,ESTU,MESANTI,ANOANTI,TIPOJOR,TC,VAL,VAN,PUENTES,JAP,JSP,HEXTRA,SALBASE,EXTRAORM,PHEXTRA,COMSAL,COMSALTT,COMSALV,IRPFMES,COTIZA,AFECMES,DIAS,SALBRUTO,PEXTRAAF,PEXTRAAV,VESP,AFECANO,MESCOMA,DIACOMA,ANOS2,FACTOTAL,CUATRIENAL) VALUES (?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?,?,?)"
    query = "INSERT INTO datosMercadoLaboral (IDENCCC, ORDENTRA, NUTS1, CNACE, ESTRATO2, CONTROL, MERCADO, REGULACION, SEXO, TIPOPAIS, CNO1, RESPONSA, ESTU, ANOANTI, MESANTI, TIPOJOR, TIPOCON, FIJODISM, FIJODISD, VAL, VAN, PUENTES, JAP, JSP1, JSP2, HEXTRA, DRELABM, SIESPM1, DSIESPM1, SIESPM2, DSIESPM2, SALBASE, EXTRAORM, PHEXTRA, COMSAL, COMSALTT, IRPFMES, COTIZA, BASE, DRELABAM, DRELABAD, SIESPA1, DSIESPA1, SIESPA2, DSIESPA2, SIESPA3, DSIESPA3, SIESPA4, DSIESPA4, RETRINOIN, RETRIIN, GEXTRA, VESPNOIN, VESPIN, ANOS2, FACTOTAL, CUATRIENAL) VALUES (:IDENCCC, :ORDENTRA, :NUTS1, :CNACE, :ESTRATO2, :CONTROL, :MERCADO, :REGULACION, :SEXO, :TIPOPAIS, :CNO1, :RESPONSA, :ESTU, :ANOANTI, :MESANTI, :TIPOJOR, :TIPOCON, :FIJODISM, :FIJODISD, :VAL, :VAN, :PUENTES, :JAP, :JSP1, :JSP2, :HEXTRA, :DRELABM, :SIESPM1, :DSIESPM1, :SIESPM2, :DSIESPM2, :SALBASE, :EXTRAORM, :PHEXTRA, :COMSAL, :COMSALTT, :IRPFMES, :COTIZA, :BASE, :DRELABAM, :DRELABAD, :SIESPA1, :DSIESPA1, :SIESPA2, :DSIESPA2, :SIESPA3, :DSIESPA3, :SIESPA4, :DSIESPA4, :RETRINOIN, :RETRIIN, :GEXTRA, :VESPNOIN, :VESPIN, :ANOS2, :FACTOTAL, :CUATRIENAL)"

    conn.executemany(query, datos)
    cursor = conn.cursor()


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