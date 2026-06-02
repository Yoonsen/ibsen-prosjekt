Her er et utkast til et arkitektur- og implementasjonsnotat som du kan legge rett inn i repoet (for eksempel som `architecture_allusjon.md` eller inkludere i manifestet ditt). Den er skrevet med tanke på både menneskelige kodere og AI-agenter (vibe coding), slik at den definerer *hva* som skal gjøres og *matematikken* bak, men overlater selve kode-implementasjonen til agenten.

---

# Arkitekturdokument: Allusjonsdeteksjon via Informasjonstetthet ($I(\text{frase})$)

## 1. Formål

Målet med denne modulen er å identifisere potensielle allusjoner (for eksempel til Ibsen) i en gitt måltekst. I stedet for å gjøre ressurskrevende vektor-søk eller brede fuzzy-søk på hele teksten, bruker vi informasjonsteori for å isolere passasjer som har en statistisk usannsynlig struktur.

Allusjoner kjennetegnes ved at de er **informasjonstunge**. Ved å beregne informasjonsverdien, $I(\text{frase})$, for rullerende vinduer (n-gram) i en tekst, kan vi filtrere bort allmennspråk og kun sitte igjen med "allusjons-ankre". Disse ankrene sendes deretter til ElasticSearch (Nettbiblioteket) for endelig verifisering.

## 2. Matematisk grunnlag

Informasjonsverdien til en frase defineres ut fra dens overraskelsesverdi (surprisal) i et gitt referansekorpus (for eksempel tekster fra 1800-tallet via dhlab).

Informasjonsverdien beregnes slik:


$$I(\text{frase}) = -\log_2 P(\text{frase})$$

Hvor sannsynligheten for frasen, $P(\text{frase})$, er gitt ved dens relative frekvens i bakgrunnskorpuset:


$$P(\text{frase}) = \frac{C(\text{frase})}{N}$$

* $C(\text{frase})$: Råfrekvensen (count) av frasen i referansekorpuset.
* $N$: Totalt antall n-gram av samme lengde i referansekorpuset.

**Tolkning:** En høy $I$-verdi betyr at frasen er sjelden og informasjonsmettet. En lav $I$-verdi betyr at frasen består av vanlige ordkombinasjoner (klisjeer, allmennspråk).

### Valgfritt tillegg: Kohesjon via PMI

For å sikre at vi ikke bare fanger opp en tilfeldig samling sjeldne ord, men en struktur der ordene "hører sammen" (som "nøgne ø"), kan Pointwise Mutual Information (PMI) brukes som et sekundært filter for n-gram:


$$\text{PMI}(x, y) = \log_2 \frac{P(x, y)}{P(x)P(y)}$$

## 3. Algoritmisk flyt for implementasjon

Modulen skal implementeres som en uavhengig pipeline med følgende steg:

### Steg 1: Tekstprosessering og n-gram-generering (Sliding Window)

Ta inn målteksten og generer kandidat-fraser.

* **Vindusstørrelse:** Generer rullerende n-gram med lengde $n \in \{2, 3, 4, 5, 6\}$.
* **Avgrensning 1 (Setninger):** Et vindu skal *aldri* krysse harde tegnsettingsgrenser (punktum, utropstegn, spørsmålstegn).
* **Avgrensning 2 (Stoppord-sandwich):** Forkast n-gram som *både* starter og slutter med rene funksjonsord/stoppord (f.eks. "i", "og", "på"), da disse sjelden utgjør kjernen i en allusjon alene.

### Steg 2: Oppslag av referansefrekvenser (DH-lab API)

For hver genererte kandidat-frase, hent ut referansefrekvensen $C(\text{frase})$ fra databasen/dhlab-ressursene.

* *Optimalisering:* Cache frekvensoppslag lokalt i minnet (eller Redis/SQLite) for å unngå gjentatte API-kall for vanlige fraser.
* *Håndtering av ukjente fraser (Out-of-vocabulary):* Hvis $C(\text{frase}) = 0$, tildel en minimal standardfrekvens (Laplace smoothing) for å unngå divisjon med null eller uendelig $I$-score.

### Steg 3: Beregning og filtrering

Beregne $I(\text{frase})$ for alle kandidater.

* Definer en terskelverdi (threshold) $T$.
* Filtrer ut alle fraser der $I(\text{frase}) < T$.
* Sitt igjen med en liste over fraser sortert synkende på $I$-verdi.

### Steg 4: Håndtering via ElasticSearch (Neste systemledd)

De isolerte høy-$I$ kandidatene eksponeres videre for applikasjonen. Disse frasene er nå klare til å sendes som søk mot ElasticSearch (api.nb.no) for å finne utbredelsen i andre tekster.

* *For koder/agent:* Denne komponenten trenger ikke kjenne til ElasticSearch. Den skal utelukkende returnere en liste over `[{"frase": "nøgne ø", "I_score": 14.5}, ...]`.

## 4. Retningslinjer for koder/AI-agent

* **Avkobling:** Hold modulen for beregning av informasjonstetthet helt adskilt fra søkemotor-logikken. Modulen tar inn en tekst og en oppslagsfunksjon (dependency injection), og returnerer strukturerte kandidater.
* **Ytelse:** Tekster kan være lange. Bruk effektive datastrukturer (f.eks. generatore/yield i Python eller streams i JS/TS) for rullerende vinduer, fremfor å bygge massive arrays i minnet.
* **Loggføring:** Sørg for at funksjonen logger et histogram over distribusjonen av $I$-verdier, slik at vi enkelt kan kalibrere terskelverdien $T$ for ulike sjangre og tidsepoker.