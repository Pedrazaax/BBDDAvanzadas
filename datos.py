import csv
import sqlite3

conn = sqlite3.connect('salarios.db')
datos = []

class cargarDatos:
    csvs =['microdatos/EES_1995.csv','microdatos/EES_2002.csv','microdatos/EES_2006.csv','microdatos/EES_2010.csv','microdatos/EES_2014.csv','microdatos/EES_2018.csv']  #Para cargar todos los csvs a la vez
    contCuatrienal= 1 #Sirve para saber en que cuatrianual estamos sabiendo que empezamos en 1995
    # la primera fila la toma para el nombre de los campos y a partir de ahi añade los datos
    for archivo in csvs:   #archivo representa al csv que se va a cargar
        with open(archivo, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                row['cuatrienal'] = contCuatrienal
                datos.append(row)
                
        contCuatrienal = contCuatrienal +1

class crearTabla:
    
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS datosMercadoLaboral")
    cursor.execute("CREATE TABLE datosMercadoLaboral (ORDENCCC INTEGER PRIMARY KEY, ORDENTRA INTEGER, NUTS1 INTEGER, CNACE TEXT,ESTRATO2 INTEGER,CONTROL INTEGER, MERCADO INTEGER, CONVENIO INTEGER,SEXO INTEGER, TIPOPAIS INTEGER, CNO1 TEXT, RESPONSA INTEGER, ESTU INTEGER, MESANTI INTEGER, ANOANTI INTEGER, TIPOJOR INTEGER, TC INTEGER, VAL INTEGER, VAN INTEGER, PUENTES INTEGER, JAP INTEGER, JSP INTEGER, HEXTRA INTEGER, SALBASE INTEGER, EXTRAORM INTEGER, PHEXTRA INTEGER, COMSAL INTEGER, COMSALTT INTEGER, COMSALV INTEGER, IRPFMES INTEGER, COTIZA INTEGER, AFECMES INTEGER, DIAS INTEGER, SALBRUTO INTEGER, PEXTRAAF INTEGER, PEXTRAAV INTEGER, VESP INTEGER, AFECANO INTEGER, MESCOMA INTEGER, DIACOMA INTEGER, ANOS2 INTEGER, FACTOTAL INTEGER, CUATRIENAL INTEGER     )")
    conn.commit()

class insercionDatos:
    query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1, CNACE, ESTRATO2, CONTROL, MERCADO, CONVENIO, SEXO, TIPOPAIS, CNO1, RESPONSA, ESTU, MESANTI, ANOANTI, TIPOJOR, TC, VAL, VAN, PUENTES, JAP, JSP, HEXTRA, SALBASE, EXTRAORM, PHEXTRA, COMSAL, COMSALTT, COMSALV, IRPFMES, COTIZA, AFECMES, DIAS, SALBRUTO, PEXTRAAF, PEXTRAAV, VESP, AFECANO, MESCOMA, DIACOMA, ANOS2, FACTOTAL, CUATRIENAL) VALUES (:ORDENCCC, :ORDENTRA, :NUTS1, :CNACE, :ESTRATO2, :CONTROL, :MERCADO, :CONVENIO, :SEXO, :TIPOPAIS, :CNO1, :RESPONSA, :ESTU, :MESANTI, :ANOANTI, :TIPOJOR, :TC, :VAL, :VAN, :PUENTES, :JAP, :JSP, :HEXTRA, :SALBASE, :EXTRAORM, :PHEXTRA, :COMSAL, :COMSALTT, :COMSALV, :IRPFMES, :COTIZA, :AFECMES, :DIAS, :SALBRUTO, :PEXTRAAF, :PEXTRAAV, :VESP, :AFECANO, :MESCOMA, :DIACOMA, :ANOS2, :FACTOTAL, :CUATRIENAL)"
    #query = "INSERT INTO datosMercadoLaboral (ORDENCCC, ORDENTRA, NUTS1,CNACE,ESTRATO2,CONTROL,MERCADO,CONVENIO,SEXO,TIPOPAIS,CNO1,RESPONSA,ESTU,MESANTI,ANOANTI,TIPOJOR,TC,VAL,VAN,PUENTES,JAP,JSP,HEXTRA,SALBASE,EXTRAORM,PHEXTRA,COMSAL,COMSALTT,COMSALV,IRPFMES,COTIZA,AFECMES,DIAS,SALBRUTO,PEXTRAAF,PEXTRAAV,VESP,AFECANO,MESCOMA,DIACOMA,ANOS2,FACTOTAL,CUATRIENAL) VALUES (?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?, ?, ?, ?, ? ,?, ?, ?, ?, ?,?,?,?)"
    conn.executemany(query, datos)

if __name__ == "__main__":
   cargarDatos()
   crearTabla()
   insercionDatos()
   print(datos[0])
   print(datos[100000]["cuatrienal"])