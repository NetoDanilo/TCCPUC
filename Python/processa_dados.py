#CÓDIGO PARA PROCESSAR E CARREGAR OS DADOS NO DATA WAREHOUSE PUCTCCDW
#VERSÃO DO PYTHON HOMOLOGADA: 3.9
#ÚLTIMA REVISÃO: 04/01/2023
#AUTOR: DANILO NETO

#BIBLIOTECAS NECESSÁRIAS PARA LER O DATASET DISPONIBILIZADO (PANDAS) E CONEXÃO COM O BANCO DE DADOS VIA ODBC (PYODBC), ALÉM DE OUTRAS DEPENDÊNCIAS.
import json
import pandas as pd
import pyodbc
import boto3
from botocore.exceptions import ClientError
import numpy as np
import io
import sys
import os

#LAMBDA HANDLER QUE PROCESSA OS EVENTOS
def lambda_handler(event, context):
    #ABRE CONEXÃO COM O AWS SYSTEMS MANAGER PARAMETER STORE PARA RECUPERAR AS CREDENCIAIS DO BANCO DE DADOS E DO USUÁRIO IAM.
    try:
        client = boto3.client('ssm')
    except ClientError as e:
        raise e    
    #RECUPERA E DECLARA A SENHA DO BANCO DE DADOS
    try:
        DB_PASSWORD = client.get_parameter(Name='DB_PASSWORD',WithDecryption=True)['Parameter']['Value']
    except ClientError as e:
        raise e
    #RECUPERA E DECLARA A AWS ACCESS KEY ID
    try:
        ACCESS_KEY_ID = client.get_parameter(Name='ACCESS_KEY_ID',WithDecryption=True)['Parameter']['Value']
    except ClientError as e:
        raise e
    #RECUPERA E DECLARA A AWS SECRET ACCESS KEY
    try:
        SECRET_ACCESS_KEY = client.get_parameter(Name='SECRET_ACCESS_KEY',WithDecryption=True)['Parameter']['Value']
    except ClientError as e:
        raise e
    
    #IP DO SERVIDOR DE BANCO DE DADOS (ARMAZENADA NO AWS LAMBDA ENVIRONMENT VARIABLES)
    SERVER_IP = os.environ['SERVER_IP']
    
    #NOME DO BANCO DE DADOS (ARMAZENADA NO AWS LAMBDA ENVIRONMENT VARIABLES)
    DATABASE = os.environ['DATABASE']
    
    #NOME DO LOGIN DE BANCO DE DADOS (ARMAZENADA NO AWS LAMBDA ENVIRONMENT VARIABLES)
    DB_LOGIN = os.environ['DB_LOGIN']
    
    #STRING DE CONEXÃO COM O BANCO DE DADOS SQL SERVER - ODBC
    conexao = (
    	"Driver={/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.2.1};"
    	"Server="+SERVER_IP+";"
    	"Database="+DATABASE+";"
    	"TrustServerCertificate=yes;"
    	"Uid="+DB_LOGIN+";"
    	"Pwd="+DB_PASSWORD+";"
    )
    
    #CAPTURAR DINAMICAMENTE O NOME DO BUCKET, NÃO É O CASO.
    #bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    #NOME DO BUCKET CRIADO
    bucket_name = 'tccpucstagingarea'
    print('Bucket utilizado: {}'.format(bucket_name))
    
    #CAPTURAR DINAMICAMENTE O NOME DO ARQUIVO IMPORTADO NO BUCKET 
    object_key = event['Records'][0]['s3']['object']['key']
    print('Arquivo que será carregado: {}'.format(object_key))
    
    #ABRE CONEXÃO COM O AMAZON S3
    try:
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
    except ClientError as e:
        raise e
    
    #OBTÉM O OBJETO
    obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    
    #LÊ O OBJETO
    data = obj['Body'].read()
    
    #ABRE CONEXÃO COM O SQL SERVER. AUTOCOMMIT ESTÁ DESLIGADO E O TIMEOUT DE CONEXÃO É DE 30 SEGUNDOS.
    try:
        mydb = pyodbc.connect(conexao,autocommit=False,timeout=30)
    except pyodbc.OperationalError as err:
        print("Error: {}".format(str(err)))
        sys.exit()
    
    #ABRE CURSOR PARA PERCORRER E PROCESSAR OS DADOS NO SQL SERVER
    cursor = mydb.cursor()
    
    #POSIÇÃO DAS COLUNAS NO DATASET QUE SERÃO UTILIZADAS
    colspos=[1,2,3,8,10,11,12,13,14,26,29,35,58,59,71,82,84,98,101,107,109,119]
    
    #NOME DAS COLUNAS
    colnames = ['iyear','imonth','iday','country_txt','region_txt','provstate','city','latitude','longitude','success','attacktype1_txt','targtype1_txt','gname','gsubname','claimed','weaptype1_txt','weapsubtype1_txt','nkill','nwound','propvalue','ishostkid','ransompaid']
    
    #TIPO DOS DADOS
    dictype={'iyear': 'int', 'imonth': 'int', 'iday': 'int','country_txt': 'str','region_txt': 'str','provstate': 'str','city': 'str','latitude': 'str','longitude': 'str','success': 'str','attacktype1_txt': 'str','targtype1_txt': 'str','gname': 'str','gsubname': 'str','claimed': 'str','weaptype1_txt': 'str','weapsubtype1_txt': 'str','nkill': 'str','nwound': 'str','propvalue': 'str','ishostkid': 'str','ransompaid': 'str','date_id': 'int','region_id': 'int','attack_id': 'int','target_id': 'int', 'gname_id': 'int','weapon_id': 'int','num_attack': 'int'}
    
    #LEITURA DO DATASET: GLOBAL TERRORISM DATABASE - START.UMD.EDU
    df = pd.read_csv(io.BytesIO(data),usecols=colspos,names=colnames,header=None,dtype=dictype,sep=',',index_col=False,engine='python',keep_default_na=False, na_values=None)
    
    #TRANSFORMA ALGUMAS COLUNAS COM O TIPO DE DADOS STR PARA NUMBER
    df["nwound"] = pd.to_numeric(df["nwound"])
    df["propvalue"] = pd.to_numeric(df["propvalue"])
    df["ransompaid"] = pd.to_numeric(df["ransompaid"])
    
    #ALTERA OS VALORES NAN (DO NUMPY) PARA NONE (O SQL SERVER NÃO ENTENDE NAN, APENAS NONE)
    df = df.replace({np.nan: None})

    print('Carga iniciada.')
    #LOOP PARA LER OS DADOS E INSERIR NAS TABELAS
    for i, linha in df.iterrows():
        #TABELA DIMENSÃO: TARGET
        target = linha['targtype1_txt']
        try:
            cursor.execute('INSERT INTO target (targtype1_txt) VALUES (?)',target)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 target_id FROM target WHERE targtype1_txt=?',target)
        resultado = cursor.fetchall()
        target_id = resultado[0][0]
        
        #TABELA DIMENSÃO: ATTACK
        attack = linha['attacktype1_txt']
        try:
            cursor.execute('INSERT INTO attack (attacktype1_txt) VALUES (?)', attack)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 attack_id FROM attack WHERE attacktype1_txt = ?', attack)
        resultado = cursor.fetchall()
        attack_id = resultado[0][0]
        
        #TABELA DIMENSÃO: REGION
        region = linha['region_txt']
        country = linha['country_txt']
        provstate = linha['provstate']
        city = linha['city']
        latitude = linha['latitude']
        longitude = linha['longitude']
        try:
            cursor.execute('INSERT INTO region (region_txt, country_txt, provstate, city,latitude,longitude) VALUES (?,?,?,?,?,?)', region, country, provstate, city,latitude,longitude)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 region_id FROM region WHERE region_txt = ? and country_txt= ? AND provstate= ? AND city= ? AND latitude = ? AND longitude = ?',region, country, provstate, city, latitude, longitude)
        resultado = cursor.fetchall()
        region_id = resultado[0][0]
        
        #TABELA DIMENSÃO: WEAPON
        weapon = linha['weaptype1_txt']
        weapsubtype = linha['weapsubtype1_txt']
        try:
            cursor.execute('INSERT INTO weapon (weaptype1_txt, weapsubtype1_txt) VALUES (?, ?)',weapon, weapsubtype)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 weapon_id FROM weapon WHERE weaptype1_txt=? AND weapsubtype1_txt=?',weapon, weapsubtype)
        resultado = cursor.fetchall()
        weapon_id = resultado[0][0]
        
        #TABELA DIMENSÃO: GNAME
        gname = linha['gname']
        gsubname = linha['gsubname']
        try:
            cursor.execute('INSERT INTO gname (gname, gsubname) VALUES (?, ?)',gname, gsubname)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 gname_id FROM gname WHERE gname = ? and gsubname = ?', gname, gsubname)
        resultado = cursor.fetchall()
        gname_id = resultado[0][0]
        
        #TABELA DIMENSÃO: DATE
        year = linha['iyear']
        month = linha['imonth']
        day = linha['iday']
        try:
            cursor.execute('INSERT INTO date (iyear, imonth, iday) VALUES (?,?,?)',year, month, day)
        except Exception:
            pass
        cursor.execute('SELECT TOP 1 date_id FROM date WHERE iyear = ? AND imonth=? AND iday=?',year, month, day)
        resultado = cursor.fetchall()
        date_id = resultado[0][0]
        
        #COLUNAS NECESSÁRIAS PARA POPULAR A TABELA FATO: EVENT
        success = linha['success']
        claimed = linha['claimed']
        ishostkid = linha['ishostkid']
        nkill = linha['nkill']
        nwound = linha['nwound']
        ransompaid = linha['ransompaid']
        propvalue = linha['propvalue']
        
        #TABELA FATO: EVENT
        cursor.execute('INSERT INTO event (date_id,  region_id, attack_id, target_id, gname_id, weapon_id, success, claimed, ishostkid, nkill, nwound,ransompaid,propvalue) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',date_id,  region_id, attack_id, target_id, gname_id, weapon_id, success, claimed, ishostkid, nkill, nwound, ransompaid,propvalue)
        mydb.commit()
    print('Carga finalizada.')
    #FECHA A CONEXÃO COM O BANCO DE DADOS
    mydb.close()
