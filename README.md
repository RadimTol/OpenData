Pracovní aplikace pro práci s OpenData ČHMÚ (opendata.chmi.cz).

Aplikace obsahuje R skripty

- update_data.R prohledá vybrané adresáře a připraví statistiku počtu souborů/stanic podle 
data v názvu jednotlivých JSON, popřípadě pro jeden z koláčových grafů (DAILY/PHENOMENA) 
podle data souborů/stanic. Připravená data jsou v souboru data.json a now_hourly.json.
- MaxMinT.R připraví soubor MaxMinT.csv s přehledem tří nejvyšší a tří nejnižších hodnot pro 
prvky T, TMA a TMI dnes (adresář NOW na opendata.chmi.cz)
- MaxMinTvcera.R připraví soubor MaxMinT_daily_last_day.csv s přehledem tří nejvyšší a tří nejnižších hodnot pro 
prvky T, TMA a TMI za včera (adresář RECENT na opendata.chmi.cz)

Aplikace obsahuje HTML soubory

- index.html je úvodní rozcestník
- statopendata.html je grafické vyjádření počtu souborů připravených skriptem update_data.R
- extremyopendata.html přehled tří nejvyšších a tří nejnižších hodnot vybraných prvků za včerejší den (aktualizováno 2x denně ve 3:15 a 15:15 UTC)
- extremyopendatadnes.html přehled tří nejvyšších a tří nejnižších hodnot vybraných prvků za dnešní den (aktualizováno postupně každou hodinu ve 25 minutě)

Pomocné soubory
- chmu_logo.svg - logo ČHMÚ do index.html
- last_run.txt a last_run_updat_date.txt - pomocnmé soubory pro kontrolu posledncíh běhů
