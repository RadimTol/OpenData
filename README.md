Pracovní aplikace pro práci s OpenData ČHMÚ (opendata.chmi.cz).

Aplikace obsahuje R skripty

- update_data.R prohledá vybrané adresáře a připraví statistiku počtu souborů/stanic podle 
data v názvu jednotlivých JSON, popřípadě pro jeden z koláčových grafů (DAILY/PHENOMENA) 
podle data souborů/stanic. Připravená data jsou v souboru data.json.

- MaxMinT.R připraví soubor MaxMinT.csv s přehledem tří nejvyšší a tří nejnižších hodnot pro 
prvky T, TMA a TMI

Aplikace obsahuje HTML soubory

- index.html je úvodní rozcestník
- 
- statopendata.html je grafické vyjádření počtu souborů připravených skriptem update_data.R
