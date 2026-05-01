# Copilot Instructions — Familiez Middleware (MW)

## Technologie
- Python 3, FastAPI, SQLAlchemy (core/text), PyMySQL
- MariaDB database: `humans`
- Omgevingsbeheer via `.env` (nooit committen); config altijd via `os.getenv()`
- Docker-container; start via `start.sh`

## Architectuur
- MW is een thin middleware: routes delegeren naar de database via stored procedures
- **Geen inline SQL in Python** — gebruik altijd `CALL sprocnaam(...)` via `sqlalchemy.text()`
- Resultaten van sprocs komen altijd terug als één of meerdere result sets
- DB-gebruiker is `HumansService` — deze heeft geen SUPER-rechten

## Authenticatie & Sessies
- SSO via LDAP/OpenID; JWT tokens; logica zit in `auth.py` en `session_manager.py`
- Bestandsroutes (`GET /api/files/*`) ondersteunen `?token=` query-param als fallback (browsers kunnen geen `Authorization` header meesturen bij `img src` / `window.open`)
- Gebruik altijd `verify_sso_token()` of `require_admin_role()` voor beveiligde routes
- Nooit tokens loggen of in responses opnemen

## Bestandsbeheer
- Bestandspaden bouwen via functies in `file_utils.py`: `get_person_path()`, `get_family_path()`, `generate_filename()`, `slugify()`
- Bestanden worden opgeslagen onder `BESTANDEN/` (staat in `.gitignore`)

## Omgevingen
- Geldige waarden voor `ENVIRONMENT`: `development`, `dev`, `test`, `staging`, `prod`, `production`
- Productie-specifiek gedrag altijd bewust achter `ENVIRONMENT in {"prod", "production"}`

## Testen
- Testbestanden: `test_main.py`, `test_auth.py`, `test_file_utils.py`, `test_marriage.py`
- Gebruik `pytest`; draai tests vanuit de MW-map met het venv actief

## Valkuilen
- Sprocs aanroepen met `SQL SECURITY INVOKER`; nooit `DEFINER=...` gebruiken
- `DatabaseURL` bevat wachtwoord via `quote_plus()` om speciale tekens te escapen
- `.env` staat nooit in git; gebruik `.env.example` als referentie
