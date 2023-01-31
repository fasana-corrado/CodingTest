# READ ME
Per alcuni dettagli circa le scelte adottate e il procedimento seguito, fare riferimento al file **"details.txt"**.

Di seguito viene mostrata la **procedura da seguire per poter testare il programma** (supponendo che python, pip e mysql siano già installati sul dispositivo).

**1) Creare un nuovo virtual environment per il progetto**
Es. in Windows usando powershell:
```
pip install virtualenv
cd PROJECT_FOLDER
virtualenv ENVIRONMENT_NAME
```

**2) Attivare il nuovo environment**
Es. in Windows usando powershell:
```
ENVIRONMENT_NAME\Scripts\activate
```

**3) Clonare la repository github all'interno della cartella (è necessario aver installato git)**
```
git clone https://github.com/fasana-corrado/CodingTest.git
```

**4) Rinominare il file '.env_example' presente nella cartella del progetto e chiamarlo '.env'.**
Dopo di che, ***modificare i dati presenti all'interno** scegliendo "USER", "PASSWORD" e "DATABASE_NAME" che dovranno poi essere creati. Per eseguire il programma in locale, usare come "HOST_NAME" 'localhost'.

**5) Creare un utente su mysql che possa svolgere qualunque funzione (creare DB, creare tabelle, inserire dati, effettuare query, etc.)**
Lo username, la password e il nome del database devono essere gli stessi di quelli inseriti nel file '.env'
```
mysql
mysql > CREATE USER 'NEW_USER'@'localhost' IDENTIFIED BY 'NEW_PASSWORD';
mysql > GRANT ALL PRIVILEGES ON NEW_DATABASE.* TO 'NEW_USER'@'localhost' WITH GRANT OPTION;
```
N.B. In a real environment, the granted privileges should be limited to avoid giving users unlimited access to the database if this is
not necessary.

**6) Installare tutte le librerie richieste usando il file 'requirements.txt' presente nella cartella del progetto.**
```
pip install -r 'requirements.txt'
```    
A questo punto è possibile accedere alle funzionalità del programma. Siccome il database è creato all'interno dello script che carica anche i dati dal file .csv, è necessario **inizializzare il database prima di poter usare le API**, altrimenti le tabelle non esistono.

**7) Eseguire lo script initialize_db.py presente nella cartella backend**
```
cd PATH_TO_BACKEND
python initialize_db.py
```   

**8) Avviare il server per rendere disponibili le api**
```
cd PATH_TO_BACKEND
uvicorn api:app
```     
Una volta avviato il server, **tutte le funzionalità sono accessibili dal notebook 'invoke_api.ipynb'** presente nella cartella 'frontend'.

**9) Avviare jupyter lab**
```
jupyter lab
```  

**10) Eseguire il notebook per testare le funzionalità**      
