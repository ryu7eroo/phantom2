# CTF Swarm

Competitive multi-model CTF solver with independent search, evidence blackboard, adaptive rounds, verification, and global cancellation.

## Architecture

- Round 1: independent solving.
- First **verified** solution emits a global solve event and cancels competing workers.
- If nobody solves, structured evidence is merged into a blackboard.
- Round 2 forces divergent approaches and avoids recorded dead ends.
- Later rounds can activate critics, specialists, and independent verifiers.

The control plane is provider-agnostic: model workers can run locally, on Azure, Camber, or another GPU host.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest -q
```
