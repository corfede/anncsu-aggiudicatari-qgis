# ANNCSU Aggiudicatari – QGIS Processing Tool

Script Processing per QGIS che collega:

* Open data PA Digitale 2026 (candidature finanziate)
* Dataset ANAC (CUP → CIG → aggiudicatari)

e produce:

* Mappa dei comuni italiani colorata per aggiudicatario
* Top operatori per numero comuni e importi finanziati

---

## Funzionalità

* filtro automatico su misura **ANNCSU**
* join:

  * Comune → CUP
  * CUP → CIG
  * CIG → aggiudicatario
* aggregazione importi finanziati
* classifica Top N operatori
* tematizzazione automatica in QGIS

---

## Input richiesti

1. **Layer comuni ISTAT**
   sorgente: https://www.istat.it/notizia/confini-delle-unita-amministrative-a-fini-statistici-al-1-gennaio-2018-2/
   * campo codice ISTAT (es. `PRO_COM_T`)
  
2. **CUP (ANAC)**
   sorgente: https://dati.anticorruzione.it/opendata/opendata/dataset/cup
   * file CSV o ZIP

3. **Aggiudicatari (ANAC)**
   sorgente: https://dati.anticorruzione.it/opendata/dataset/aggiudicatari
   * file CSV

---

## Output

* GeoPackage con:

  * layer comuni con:
    - aggiudicatario
    - CUP
    - CIG
    - importo finanziato
  * tabella Top operatori

---

## Installazione

In QGIS:
Processing → Toolbox → Scripts → Add script from file

Selezionare:
anncsu-aggiudicatari.py

## Note

* I dataset ANAC devono essere file CSV reali (non pagine web)
* I file possono essere molto grandi → tempi di esecuzione lunghi
* Non tutti i CUP hanno CIG associato
* Non tutti i CIG hanno aggiudicatario disponibile

---

## Fonte dati

* PA Digitale 2026 Open Data
* ANAC Open Data

---

## Scopo

Questo tool ricostruisce una relazione **non esplicita nei dati pubblici**:
finanziamenti → gare → aggiudicatari

---

## Autore

Federico Cortese

## Crediti

Il flusso di lavoro per l’incrocio tra dataset ANNCSU e CIG degli aggiudicatari è stato ispirato dal lavoro di Dennis Angemi
Lo script è stato sviluppato con il supporto di ChatGPT (modello GPT-5.x, OpenAI)
