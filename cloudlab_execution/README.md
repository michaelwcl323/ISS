# ISS CloudLab execution

This directory provides the same direct CloudLab workflow used by Ladon:

- one controller runs `discoverymaster` and the client process;
- each configured CloudLab host runs one consensus peer;
- configuration is generated from ISS's existing deployment scripts;
- peer output is collected and analyzed locally.

For experimental parity with Ladon, direct CloudLab runs override `UseTLS` to
`false` in generated run-specific configuration. ISS's default template is not
modified, and client request signing remains controlled independently by
`authentication` in `run_experiment.py`. Legacy `tc eth0` bandwidth commands
are also replaced with no-ops because CloudLab interface names vary by profile.

The controller generates the client request-signing key pair once. Its public
key is then installed on every peer after that peer generates its own local
TLS/authentication certificate. This preserves ISS's real request signature
verification while allowing peer TLS identities to remain node-specific.

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
