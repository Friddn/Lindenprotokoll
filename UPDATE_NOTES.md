# Update notes – Lindenprotokoll 0.4.20

This release is an additive update to the existing application. It includes the shared six-digit PIN gate, hourly heat-pump and Tornesch weather comparison, daily electricity breakdown, screenshot archiving and estimated curve digitization, plus the corrections documented in `NEXT_COMMIT_NOTES.md`.

## Important data protection

- Existing SQLite tables and historical entries are preserved.
- New feature tables and security state are created additively.
- Do not delete or overwrite the Home Assistant `/data` directory.
- Back up the complete `/data` directory before updating.

## Initial PIN setup

On the first startup after the security migration, use the temporary PIN `000000` once and immediately choose a different six-digit PIN. The PIN is stored as a password hash. Subsequent PIN changes require the current PIN in Settings.

## Testing

Python syntax and synthetic graph-digitizer tests passed. Full Home Assistant runtime/build testing remains necessary on a test copy before production installation.


## Version 0.4.21 – Home Assistant build fix

- Bumped the add-on version to 0.4.21 because 0.4.20 was already attempted.
- Replaced the unset BUILD_FROM dependency with the explicit multi-architecture
  Home Assistant base image `ghcr.io/home-assistant/base:latest`.
- This addresses the Supervisor build error where BUILD_FROM was empty.
- No application database or `/data` migration is included in this Dockerfile-only fix.
- A successful build and runtime test on the target Home Assistant installation
  are still required before production deployment.
