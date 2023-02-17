import csv

csvs =['EES_2018.csv']  #Para cargar todos los csvs a la vez
datos = []

# la primera fila la toma para el nombre de los campos y a partir de ahi añade los datos
for archivo in csvs:   #archivo representa al csv que se va a cargar
    with open(archivo, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            datos.append(row)
            #a la row hay que añadirle en que cuatrianual esta
            
    ##Aqui añade los datos a las tablas (antes de que termine el for)


print(datos[0])
print(datos[0]["ORDENTRA"])