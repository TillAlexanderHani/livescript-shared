# livescript-shared — Brief Podcasts Macro

Pipeline automatisé qui source des podcasts macro/finance, les transcrit
(Whisper), les résume avec **Claude** (via l'abonnement Claude Pro/Max — pas
d'API payante) et envoie **un seul email digest par matin**, trié par
pertinence pour un trader de devises émergentes (FX Asie + LATAM).

Tout tourne gratuitement sur **GitHub Actions** : aucune machine à laisser
allumée, aucun coût au-delà de l'abonnement Claude.

## Comment ça marche

- **GitHub Actions** déclenche le pipeline plusieurs fois par jour (heures UTC) :
  - `05:00 UTC` → mode **digest** : collecte les nouveautés puis envoie le brief du matin (~07:00 Paris).
  - `09/13/17/21 UTC` → mode **collect** : transcrit + résume les nouveaux épisodes et les met en file d'attente (aucun email).
- `podcast_pipeline.py` : tout le pipeline.
- `emailed_episodes.json` : base anti-doublon (mémoire de tout ce qui a déjà été traité — **ne pas supprimer**).
- `pending_digest.json` : épisodes résumés en attente d'envoi + date du dernier digest.

## Configuration (une seule fois)

### 1. Générer le token Claude (abonnement, sans API payante)

Sur ta machine, Claude Code installé et connecté à ton compte Pro :

```bash
claude setup-token
```

Ça génère un token longue durée `sk-ant-oat01-...`. Il n'utilise QUE ton quota
d'abonnement (refusé par l'API Messages payante). Copie-le.

### 2. Renseigner les secrets GitHub

Repo → **Settings → Secrets and variables → Actions → New repository secret** :

| Secret | Valeur |
|---|---|
| `CLAUDE_CODE_OAUTH_TOKEN` | le token `sk-ant-oat01-...` de l'étape 1 |
| `EMAIL_FROM` | adresse Gmail expéditrice |
| `EMAIL_TO` | adresse de réception du digest |
| `EMAIL_PASSWORD` | **mot de passe d'application** Gmail (pas le mot de passe du compte) |

> Gmail : crée un mot de passe d'application sur https://myaccount.google.com/apppasswords
> (la validation en deux étapes doit être activée).

### 3. Tester

Repo → onglet **Actions** → *Podcast Pipeline (Claude)* → **Run workflow** →
choisir `mode = digest` (ou `collect` pour tester sans envoyer d'email).

## Réglages (variables d'environnement, optionnel)

| Variable | Défaut | Rôle |
|---|---|---|
| `RUN_MODE` | `collect` | `collect`, `digest` ou `both` (posé par le workflow) |
| `WHISPER_MODEL` | `base` | modèle Whisper (`tiny`/`base`/`small`) |
| `CLAUDE_MODEL` | défaut Claude Code | ex. forcer un modèle précis |
| `MAX_RECENT_DAYS` | `5` | ne traite que les épisodes récents |
| `EPISODES_PER_FEED` | `1` | nb max d'épisodes par flux et par run |

## Ajouter un podcast

Ajoute une entrée `"Nom": "URL_RSS"` dans `RSS_FEEDS` (et un mapping éditeur
dans `PROVIDER_MAPPINGS` si besoin). Astuce pour trouver le vrai flux RSS :

```bash
curl -s "https://itunes.apple.com/search?term=NOM+DU+PODCAST&entity=podcast&limit=3" \
  | python3 -c "import sys,json;[print(r['collectionName'],r['feedUrl']) for r in json.load(sys.stdin)['results']]"
```

## Quota Claude Pro

Les synthèses consomment ton quota d'abonnement (partagé avec ton usage
interactif). Pour rester dans les clous : `EPISODES_PER_FEED=1`, fenêtre de
5 jours, et la transcription Whisper tourne côté GitHub (gratuite, hors quota).
Si tu tapes les limites, espace les crons ou réduis le nombre de flux.
