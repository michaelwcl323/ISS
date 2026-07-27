# ISS CloudLab execution

This directory provides the same direct CloudLab workflow used by Ladon:

- one controller runs `discoverymaster` and the client process;
- each configured CloudLab host runs one consensus peer;
- configuration is generated from ISS's existing deployment scripts;
- peer output is collected and analyzed locally.

## Configuration

Copy `cloudlab_settings.example.json` to `cloudlab_settings.json`, then set:

- `key.path`: the SSH private key;
- `repo`: the ISS repository URL and branch installed on every node;
- `hosts`: peer SSH addresses, usernames, ports, and private addresses.

The controller address and experiment parameters are intentionally kept near
the beginning of `remote()` in `run_experiment.py`. The default controller is
`10.10.1.11`, while the example uses `10.10.1.1` through `10.10.1.10` as peers.

If the SSH key has a passphrase, unlock it in the same shell before running:

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/cloudlab_key
```

## Run

Install/update ISS and its pinned build environment on all selected nodes:

```bash
python3 cloudlab_execution/run_experiment.py --remote --install
```

Run after installation:

```bash
python3 cloudlab_execution/run_experiment.py --remote
```

Connectivity and environment check without starting an experiment:

```bash
python3 cloudlab_execution/run_experiment.py --remote --dry-run
```

The local mode uses `~/iss-gopath`:

```bash
python3 cloudlab_execution/setup_environment.py
source ~/.bashrc
python3 cloudlab_execution/run_experiment.py --local
```

Remote results are written under
`deployment/deployment-data/direct-remote-NNNN/`.
