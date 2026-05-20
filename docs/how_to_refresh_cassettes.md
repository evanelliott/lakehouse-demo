To record or regenerate your local VCR network cassettes, you must perform the execution from your host machine (Mac/Windows terminal), outside the active sandboxed VS Code development container.
Because your repository uses a strict air-gapped configuration (--record-mode=none) during standard local pre-commit cycles [docs/test_approach_3.md], the test runner will prevent live internet connections to avoid environment contamination.
------------------------------
## Step-by-Step Cassette Regeneration Guide## 1. Clean the Stale Cassette Cache
Before recording clean network interactions, purge old or incomplete mock tracking files to prevent merge or state contamination. Run this command inside your project root folder on your host machine:

rm -rf tests/unit/scraper/cassettes/*

## 2. Open the Outbound Internet Bridge
Execute the target testing block using the ephemeral production-simulation profile (profile: test) defined in your docker-compose.yml [docker-compose.yml]. By overriding the test command with --record-mode=once, you allow vcrpy to open a live outbound socket to understat.com and premierinjuries.com, resolve the requests, and save them.
Run this command from your Host Machine Terminal:

docker compose run --rm -e INTEGRATION_TEST_MODE=false integration-tester pytest tests/unit/scraper/test_scraper_network_handshake.py -m net --record-mode=once

## 🔍 What this command does behind the scenes:

* run --rm: Spawns an isolated pre-push testing runner container and tears it down instantly once execution finishes to guarantee complete statelessness [docker-compose.yml].
* -e INTEGRATION_TEST_MODE=false: Forces your session conftest.py engine factory to connect over the lightweight local loopback daemon instead of spinning up heavy stateful JVM instances [tests/unit/conftest.py].
* --record-mode=once: Instructs the recording engine to establish outbound internet paths, execute the true two-step browser and cookie handshake sequences, capture the raw response payloads, and freeze them to disk as text-based YAML logs.
* Volume Mapping: Because your repository mounts your live directories cached (.:/app:cached), the generated files are instantly saved directly back onto your host disk [docker-compose.yml].

------------------------------
## Expected Project Mappings
Once the execution clears, verify that the VCR playback engine has automatically compiled your network paths into these exact text tracking configurations:

tests/unit/scraper/cassettes/
└── test_scraper_network_handshake/
    ├── test_understat_handshake_success[https:--understat.com-league-EPL-2025-https:--understat.com-getLeagueData-EPL-2025].yaml
    ├── test_understat_handshake_success[https:--understat.com-match-29088-https:--understat.com-getMatchData-29088].yaml
    └── test_premier_injuries_handshake_success.yaml

------------------------------
## Verifying Offline Pre-Commit Compliance
Now that those frozen YAML interaction captures are securely tracked inside your workspace file structure, close out your host terminal, drop back inside your sandboxed container, and run your automated pre-commit hook validator:

bash scripts/pre_commit.sh

The test runner will automatically find your recorded cassettes and playback the network loops in under 0.5 seconds with 100% offline accuracy, keeping your air-gapped development gate secure.
------------------------------
## 🚀 Advancing Your Platform Architecture
Now that your scraper network client, parsing engines, utilities, and validation suites are completely green, let me know if you would like to proceed with:

* Implementing the SCD Type 2 player injury history stitching timeline logic (test_injuries.py) to verify history modifications.
* Mapping out the DuckDB query validation tests for your idempotent state validation checks.


